package zonesync

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type synchronizedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *synchronizedLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *synchronizedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestMasterStreamTracksNodeLifecycle(t *testing.T) {
	dir := t.TempDir()
	writeZone(t, dir, "example.com.json", testZone)
	master, err := NewMaster(dir, "secret")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(StreamPath, master.ServeStream)
	mux.HandleFunc(NodesPath, master.ServeNodes)
	server := httptest.NewServer(mux)
	defer server.Close()

	var logs synchronizedLogBuffer
	previousOutput := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previousOutput)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- master.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("master watcher: %s", err)
		}
	}()

	streamURL := "ws" + strings.TrimPrefix(server.URL, "http") + StreamPath + "?id=node-1"
	if conn, resp, err := websocket.DefaultDialer.Dial(streamURL, nil); err == nil {
		conn.Close()
		t.Fatal("stream accepted a connection without a token")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("stream without token: response = %v, error = %v", resp, err)
	} else {
		resp.Body.Close()
	}
	statusURL := server.URL + NodesPath
	if resp, err := http.Get(statusURL); err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("node status without token: HTTP %d", resp.StatusCode)
		}
	}

	header := http.Header{"Authorization": []string{"Bearer secret"}}
	dial := func() *websocket.Conn {
		t.Helper()
		conn, resp, err := websocket.DefaultDialer.Dial(streamURL, header)
		if err != nil {
			t.Fatalf("connect node: response = %v, error = %v", resp, err)
		}
		return conn
	}
	status := func() NodeStatus {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, statusURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var nodes []NodeStatus
		if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
			t.Fatal(err)
		}
		if len(nodes) != 1 || nodes[0].ID != "node-1" {
			t.Fatalf("node status = %+v", nodes)
		}
		return nodes[0]
	}
	waitStatus := func(check func(NodeStatus) bool) NodeStatus {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			got := status()
			if check(got) {
				return got
			}
			if time.Now().After(deadline) {
				t.Fatalf("node status did not reach expected state: %+v", got)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	conn := dial()
	var first snapshot
	if err := conn.ReadJSON(&first); err != nil {
		t.Fatal(err)
	}
	if first.Revision == "" || len(first.Zones) != 1 {
		t.Fatalf("initial snapshot = %+v", first)
	}
	if err := conn.WriteJSON(acknowledgement{Revision: first.Revision}); err != nil {
		t.Fatal(err)
	}
	waitStatus(func(s NodeStatus) bool {
		return s.Connected && s.AppliedRevision == first.Revision && !s.LastSeen.IsZero()
	})

	writeZone(t, dir, "new.example.json", testZone)
	if err := os.Remove(filepath.Join(dir, "example.com.json")); err != nil {
		t.Fatal(err)
	}
	want, err := readSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		var next snapshot
		if err := conn.ReadJSON(&next); err != nil {
			t.Fatal(err)
		}
		if next.Revision == want.Revision {
			if len(next.Zones) != 1 || next.Zones["new.example.json"] == nil {
				t.Fatalf("updated snapshot = %+v", next)
			}
			if err := conn.WriteJSON(acknowledgement{Revision: next.Revision, Error: "reload failed"}); err != nil {
				t.Fatal(err)
			}
			waitStatus(func(s NodeStatus) bool { return s.LastError == "reload failed" && s.AppliedRevision == first.Revision })
			if err := conn.WriteJSON(acknowledgement{Revision: next.Revision}); err != nil {
				t.Fatal(err)
			}
			waitStatus(func(s NodeStatus) bool { return s.LastError == "" && s.AppliedRevision == next.Revision })
			break
		}
	}
	conn.Close()
	waitStatus(func(s NodeStatus) bool { return !s.Connected && strings.Contains(s.LastError, "connection closed") })

	reconnected := dial()
	var latest snapshot
	if err := reconnected.ReadJSON(&latest); err != nil {
		t.Fatal(err)
	}
	if latest.Revision != want.Revision {
		t.Fatalf("reconnected snapshot revision = %q, want %q", latest.Revision, want.Revision)
	}
	waitStatus(func(s NodeStatus) bool { return s.Connected && s.LastError == "" && s.AppliedRevision == want.Revision })
	replacement := dial()
	var replacementSnapshot snapshot
	if err := replacement.ReadJSON(&replacementSnapshot); err != nil {
		t.Fatal(err)
	}
	if replacementSnapshot.Revision != want.Revision {
		t.Fatalf("replacement snapshot revision = %q, want %q", replacementSnapshot.Revision, want.Revision)
	}
	reconnected.Close()
	if current := status(); !current.Connected {
		t.Fatalf("replaced connection marked node offline: %+v", current)
	}
	replacement.Close()
	waitStatus(func(s NodeStatus) bool { return !s.Connected })
	for _, event := range []string{"node \"node-1\" connected", "node \"node-1\" failed to apply", "node \"node-1\" disconnected", "node \"node-1\" reconnected", "node \"node-1\" replaced connection"} {
		deadline := time.Now().Add(3 * time.Second)
		for !strings.Contains(logs.String(), event) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if !strings.Contains(logs.String(), event) {
			t.Errorf("missing log event %q in %s", event, logs.String())
		}
	}
}

func TestFollowerAnswersHeartbeatWhileApplying(t *testing.T) {
	masterDir := t.TempDir()
	followerDir := t.TempDir()
	writeZone(t, masterDir, "example.com.json", testZone)
	master, err := NewMaster(masterDir, "secret")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(master.ServeStream))
	defer server.Close()
	follower, err := NewFollower(followerDir, server.URL, "secret", "slow-node")
	if err != nil {
		t.Fatal(err)
	}
	applyStarted := make(chan struct{})
	releaseApply := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseApply) }) }
	defer release()
	follower.SetReload(func() error {
		close(applyStarted)
		<-releaseApply
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		follower.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		release()
		<-done
	}()
	select {
	case <-applyStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("follower did not start applying the snapshot")
	}

	master.mu.Lock()
	p := master.peers["slow-node"]
	before := master.statuses["slow-node"].LastSeen
	master.mu.Unlock()
	if p == nil {
		t.Fatal("follower is not connected")
	}
	if err := p.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		master.mu.Lock()
		lastSeen := master.statuses["slow-node"].LastSeen
		master.mu.Unlock()
		if lastSeen.After(before) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower did not answer ping during zone reload")
		}
		time.Sleep(10 * time.Millisecond)
	}
	release()
	deadline = time.Now().Add(3 * time.Second)
	for {
		master.mu.Lock()
		applied := master.statuses["slow-node"].AppliedRevision
		master.mu.Unlock()
		if applied == master.latest.Revision {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower did not acknowledge the revision after reload")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
