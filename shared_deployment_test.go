package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/abh/geodns/v3/appconfig"
	"gopkg.in/gcfg.v1"
)

func TestSharedDeploymentBootstrapAddHotWeightRemoveAndRestart(t *testing.T) {
	clientPort, peerPort := reserveClusterPort(t), reserveClusterPort(t)
	for peerPort == clientPort {
		peerPort = reserveClusterPort(t)
	}
	source := t.TempDir()
	zone := []byte(`{"data":{"www":{"a":[["192.0.2.1",1]]}}}`)
	if err := os.WriteFile(filepath.Join(source, "example.com.json"), zone, 0644); err != nil {
		t.Fatal(err)
	}
	text := func(count int, high int, remove int) string {
		var b strings.Builder
		bootstrap := 1
		if remove == 1 {
			bootstrap = 2
		}
		fmt.Fprintf(&b, "[cluster]\nenabled = true\ntoken = secret\nbootstrap = node-%d\nclient-port = %d\npeer-port = %d\n", bootstrap, clientPort, peerPort)
		for i := 1; i <= count; i++ {
			if i == remove {
				continue
			}
			weight := 400 - i*100
			if i == high {
				weight = 500
			}
			fmt.Fprintf(&b, "\n[node \"node-%d\"]\naddress = 127.0.0.%d\nlisten = 192.0.2.%d\nweight = %d\n", i, 30+i, i, weight)
			if i == 1 {
				fmt.Fprintf(&b, "zone-directory = %s\n", source)
			}
		}
		return b.String()
	}
	type running struct {
		ha   *haNode
		dir  string
		url  string
		cfg  appconfig.AppConfig
		done chan error
		stop func()
	}
	start := func(i int, content, dir string) *running {
		t.Helper()
		file := filepath.Join(dir, "geodns.conf")
		if err := os.WriteFile(file, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		var cfg appconfig.AppConfig
		if err := gcfg.ReadFileInto(&cfg, file); err != nil {
			t.Fatal(err)
		}
		// Loopback hosts all share one OS in this test; deployments auto-select by WG IP.
		cfg.Cluster.ID = fmt.Sprintf("node-%d", i)
		httpServer := httptest.NewUnstartedServer(nil)
		node, err := newHANode(cfg, dir, file, httpServer.Listener.Addr().String(), true)
		if err != nil {
			t.Fatal(err)
		}
		node.embedded.config.TickMs, node.embedded.config.ElectionMs = 50, 500
		httpServer.Config.Handler = NewHTTPServer(nil, serverInfo, node.controller).Mux()
		httpServer.Start()
		ctx, cancel := context.WithCancel(context.Background())
		done, followerDone := make(chan error, 1), make(chan struct{})
		go func() { done <- node.Run(ctx) }()
		go func() { node.follower.Run(ctx); close(followerDone) }()
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			cancel()
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Error("shared node shutdown timed out")
			}
			<-followerDone
			httpServer.Close()
			node.store.Close()
		}
		t.Cleanup(stop)
		return &running{ha: node, dir: dir, url: httpServer.URL, cfg: cfg, done: done, stop: stop}
	}
	wait := func(check func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatal("shared deployment did not reach expected state")
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	initial := text(3, 0, 0)
	// An ordinary node starting before the bootstrap must wait, never form its own cluster.
	second := start(2, initial, t.TempDir())
	time.Sleep(250 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(second.ha.embedded.config.Dir, "member", "wal")); !os.IsNotExist(err) {
		t.Fatalf("non-bootstrap created an independent store: %v", err)
	}
	first := start(1, initial, t.TempDir())
	third := start(3, initial, t.TempDir())
	client, err := first.ha.embedded.client()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitCount := func(count int) {
		t.Helper()
		wait(func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resp, err := client.MemberList(ctx)
			if err != nil || len(resp.Members) != count {
				return false
			}
			for _, member := range resp.Members {
				if member.IsLearner {
					return false
				}
			}
			return true
		})
	}
	waitLeader := func(node *running) {
		t.Helper()
		wait(func() bool {
			url, err := node.ha.store.LeaderURL(context.Background())
			return err == nil && url == node.url && node.ha.controller.IsLeader()
		})
	}
	waitZone := func(node *running, data []byte) {
		t.Helper()
		wait(func() bool {
			got, err := os.ReadFile(filepath.Join(node.dir, "example.com.json"))
			return err == nil && bytes.Equal(got, data)
		})
	}
	waitCount(3)
	waitLeader(first)
	for _, node := range []*running{first, second, third} {
		waitZone(node, zone)
	}
	// Declare a new member using only a file edit on a non-master.
	updated := text(4, 0, 0)
	file := filepath.Join(second.dir, "geodns.conf")
	save := func(data string) {
		t.Helper()
		tmp := file + ".tmp"
		if err := os.WriteFile(tmp, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, file); err != nil {
			t.Fatal(err)
		}
	}
	save(updated)
	key := first.ha.embedded.deployment.key()
	wait(func() bool {
		resp, err := client.Get(context.Background(), key)
		return err == nil && len(resp.Kvs) == 1 && bytes.Contains(resp.Kvs[0].Value, []byte(`"node-4"`))
	})
	// A declared but offline member must not count toward quorum yet.
	waitCount(3)
	fourth := start(4, updated, t.TempDir())
	waitCount(4)
	waitZone(fourth, zone)
	// An edit to an old three-node file must preserve the fourth member, whose
	// declaration arrived from a different host.
	firstFile := filepath.Join(first.dir, "geodns.conf")
	if err := os.WriteFile(firstFile, []byte(strings.Replace(initial, "weight = 200", "weight = 400", 1)), 0644); err != nil {
		t.Fatal(err)
	}
	waitLeader(second)
	waitCount(4)
	// All other files are still old, but the shared weight update applies remotely.
	save(text(4, 3, 0))
	waitLeader(third)
	bad := strings.Replace(text(4, 3, 0), "address = 127.0.0.31", "address = 127.0.0.39", 1)
	save(bad)
	time.Sleep(1500 * time.Millisecond)
	resp, err := client.Get(context.Background(), key)
	if err != nil || bytes.Contains(resp.Kvs[0].Value, []byte("127.0.0.39")) {
		t.Fatal("invalid WG address change published")
	}
	// Recovery with an unchanged stale local file must retain the newer published weight.
	third.stop()
	waitLeader(second)
	third = start(3, initial, third.dir)
	waitLeader(third)
	waitCount(4)
	// Offline edits are distinguished from an unchanged stale local file.
	third.stop()
	third = start(3, strings.Replace(text(3, 3, 0), "weight = 500", "weight = 600", 1), third.dir)
	wait(func() bool {
		resp, err := client.Get(context.Background(), key)
		if err != nil || len(resp.Kvs) == 0 {
			return false
		}
		spec, err := third.ha.embedded.deployment.decode(resp.Kvs[0].Value)
		return err == nil && spec.Nodes["node-3"].Weight == 600 && spec.Nodes["node-4"] != nil
	})
	waitLeader(third)
	newZone := []byte(strings.ReplaceAll(string(zone), "192.0.2.1", "192.0.2.2"))
	if err := os.WriteFile(filepath.Join(source, "example.com.json"), newZone, 0644); err != nil {
		t.Fatal(err)
	}
	for _, node := range []*running{first, second, third, fourth} {
		waitZone(node, newZone)
	}
	// Remove a live member by deleting its section, with no management command.
	save(text(4, 3, 4))
	waitCount(3)
	select {
	case err := <-fourth.done:
		if err == nil {
			t.Error("removed embedded member should end its HA runtime")
		}
		// Let cleanup observe the already consumed completion.
		fourth.done <- err
	case <-time.After(15 * time.Second):
		t.Fatal("removed node stayed running")
	}
	fourth.stop()
	// A stale config with its original WAL must not add the removed voter again.
	if err := fourth.ha.embedded.deployment.admit(context.Background(), client); err == nil {
		t.Fatal("removed node re-admitted from stale config")
	}
	// Shrink 3 -> 2 -> 1 while deleted members are online until removal commits.
	save(text(3, 0, 3))
	waitCount(2)
	waitLeader(second)
	var two appconfig.AppConfig
	if err := gcfg.ReadStringInto(&two, text(2, 0, 2)); err != nil {
		t.Fatal(err)
	}
	data, err := deploymentData(two)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(context.Background(), key, string(data)); err != nil {
		t.Fatal(err)
	}
	waitCount(1)
	waitLeader(first)
}
