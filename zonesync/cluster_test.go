package zonesync

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func testEtcdURL(t *testing.T) url.URL {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	return url.URL{Scheme: "http", Host: addr}
}

func testEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	config := embed.NewConfig()
	config.Dir = t.TempDir()
	config.LogLevel = "error"
	config.LogOutputs = []string{os.DevNull}
	peer := testEtcdURL(t)
	client := testEtcdURL(t)
	config.ListenPeerUrls = []url.URL{peer}
	config.AdvertisePeerUrls = []url.URL{peer}
	config.ListenClientUrls = []url.URL{client}
	config.AdvertiseClientUrls = []url.URL{client}
	config.InitialCluster = config.InitialClusterFromName(config.Name)
	server, err := embed.StartEtcd(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		t.Fatal("embedded etcd did not become ready")
	}
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{client.String()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { etcdClient.Close() })
	return etcdClient
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("expected state was not reached")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStorePublishesChunkedSnapshotsAndPrunesHistory(t *testing.T) {
	client := testEtcd(t)
	store, err := NewStore(client, "/test/store")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeZone(t, dir, "example.com.json", testZone)
	first, err := store.Publish(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	large := `{"data":{"www":{"a":[["192.0.2.1",1]]}},"padding":"` + strings.Repeat("x", snapshotChunkSize+100) + `"}`
	writeZone(t, dir, "large.example.json", large)
	second, err := store.Publish(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := store.loadCurrent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != second || string(got.Zones["large.example.json"]) != large {
		t.Fatal("chunked snapshot did not round-trip")
	}
	writeZone(t, dir, "example.com.json", strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.2"))
	third, err := store.Publish(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || second == third {
		t.Fatal("different zone data reused a revision")
	}
	resp, err := client.Get(context.Background(), store.snapshotPrefix(first), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatal("old snapshot was not pruned")
	}
	resp, err = client.Get(context.Background(), store.snapshotPrefix(second), clientv3.WithPrefix())
	if err != nil || len(resp.Kvs) < 2 {
		t.Fatalf("previous snapshot was not retained: %v, %d chunks", err, len(resp.Kvs))
	}
	writeZone(t, dir, "example.com.json", `{"data":{"www":{"a":[["192.0.2.1",1]]}},"targeting":"invalid-target"}`)
	if _, err := store.Publish(context.Background(), dir); err == nil {
		t.Fatal("invalid zone was published")
	}
	got, _, err = store.loadCurrent(context.Background())
	if err != nil || got.Revision != third {
		t.Fatalf("failed publish changed active revision: %s, %v", got.Revision, err)
	}
}

func TestClusterFailoverAndFollowerReconnect(t *testing.T) {
	client := testEtcd(t)
	store, err := NewStore(client, "/test/cluster")
	if err != nil {
		t.Fatal(err)
	}
	masterDir := t.TempDir()
	followerDir := t.TempDir()
	writeZone(t, masterDir, "example.com.json", testZone)
	firstController, err := NewCluster(store, "controller-1", "secret")
	if err != nil {
		t.Fatal(err)
	}
	secondController, err := NewCluster(store, "controller-2", "secret")
	if err != nil {
		t.Fatal(err)
	}
	serveCluster := func(cluster *Cluster) *httptest.Server {
		mux := http.NewServeMux()
		mux.HandleFunc(StreamPath, cluster.ServeStream)
		mux.HandleFunc(NodesPath, cluster.ServeNodes)
		return httptest.NewServer(mux)
	}
	firstServer := serveCluster(firstController)
	defer firstServer.Close()
	secondServer := serveCluster(secondController)
	defer secondServer.Close()
	firstCtx, stopFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstController.Run(firstCtx) }()
	firstStopped := false
	defer func() {
		stopFirst()
		if !firstStopped {
			if err := <-firstDone; err != nil {
				t.Errorf("first controller: %s", err)
			}
		}
	}()
	statusCode := func(base string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, base+NodesPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	eventually(t, func() bool {
		resp, err := client.Get(context.Background(), store.prefix+"/election", clientv3.WithPrefix())
		return err == nil && len(resp.Kvs) == 1
	})
	if got := statusCode(firstServer.URL); got != http.StatusServiceUnavailable {
		t.Fatalf("controller without published snapshot: HTTP %d", got)
	}
	first, err := store.Publish(context.Background(), masterDir)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return statusCode(firstServer.URL) == http.StatusOK })
	secondCtx, stopSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- secondController.Run(secondCtx) }()
	defer func() {
		stopSecond()
		if err := <-secondDone; err != nil {
			t.Errorf("second controller: %s", err)
		}
	}()
	eventually(t, func() bool { return statusCode(secondServer.URL) == http.StatusServiceUnavailable })
	follower, err := NewFollowerWithURLs(followerDir, []string{firstServer.URL, secondServer.URL}, "secret", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	followerCtx, stopFollower := context.WithCancel(context.Background())
	followerDone := make(chan struct{})
	go func() {
		follower.Run(followerCtx)
		close(followerDone)
	}()
	defer func() {
		stopFollower()
		<-followerDone
	}()
	followerHasIP := func(ip string) bool {
		data, err := os.ReadFile(filepath.Join(followerDir, "example.com.json"))
		return err == nil && strings.Contains(string(data), ip)
	}
	eventually(t, func() bool { return followerHasIP("192.0.2.1") })
	offlineURL := "ws" + strings.TrimPrefix(firstServer.URL, "http") + StreamPath + "?id=offline-node"
	offlineConn, _, err := websocket.DefaultDialer.Dial(offlineURL, http.Header{"Authorization": []string{"Bearer secret"}})
	if err != nil {
		t.Fatal(err)
	}
	var offlineSnapshot snapshot
	if err := offlineConn.ReadJSON(&offlineSnapshot); err != nil {
		t.Fatal(err)
	}
	offlineConn.Close()
	eventually(t, func() bool {
		statuses, err := store.loadNodeStatuses(context.Background())
		status, ok := statuses["offline-node"]
		return err == nil && ok && !status.Connected
	})
	if first == "" {
		t.Fatal("initial published revision is empty")
	}
	stopFirst()
	err = <-firstDone
	firstStopped = true
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return statusCode(firstServer.URL) == http.StatusServiceUnavailable && statusCode(secondServer.URL) == http.StatusOK
	})
	req, err := http.NewRequest(http.MethodGet, secondServer.URL+NodesPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var nodes []NodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	foundOffline := false
	for _, node := range nodes {
		if node.ID == "offline-node" && !node.Connected {
			foundOffline = true
		}
	}
	if !foundOffline {
		t.Fatalf("new leader forgot offline node: %+v", nodes)
	}
	fallbackDir := t.TempDir()
	fallback, err := NewFollowerWithURLs(fallbackDir, []string{firstServer.URL, secondServer.URL}, "secret", "node-2")
	if err != nil {
		t.Fatal(err)
	}
	fallbackCtx, stopFallback := context.WithCancel(context.Background())
	fallbackDone := make(chan struct{})
	go func() {
		fallback.Run(fallbackCtx)
		close(fallbackDone)
	}()
	defer func() {
		stopFallback()
		<-fallbackDone
	}()
	eventually(t, func() bool {
		data, err := os.ReadFile(filepath.Join(fallbackDir, "example.com.json"))
		return err == nil && strings.Contains(string(data), "192.0.2.1")
	})
	writeZone(t, masterDir, "example.com.json", strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.2"))
	if _, err := store.Publish(context.Background(), masterDir); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return followerHasIP("192.0.2.2") })
}

func TestClusterLeaseExpiresAfterControllerLosesEtcd(t *testing.T) {
	baseClient := testEtcd(t)
	newClient := func() *clientv3.Client {
		t.Helper()
		client, err := clientv3.New(clientv3.Config{Endpoints: baseClient.Endpoints(), DialTimeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	firstClient := newClient()
	defer firstClient.Close()
	secondClient := newClient()
	defer secondClient.Close()
	publisher, err := NewStore(baseClient, "/test/lease-loss")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeZone(t, dir, "example.com.json", testZone)
	if _, err := publisher.Publish(context.Background(), dir); err != nil {
		t.Fatal(err)
	}
	firstStore, err := NewStore(firstClient, "/test/lease-loss")
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := NewStore(secondClient, "/test/lease-loss")
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewCluster(firstStore, "first", "secret")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCluster(secondStore, "second", "secret")
	if err != nil {
		t.Fatal(err)
	}
	first.leaseTTL = 3
	second.leaseTTL = 3
	firstHTTP := httptest.NewServer(http.HandlerFunc(first.ServeNodes))
	defer firstHTTP.Close()
	secondHTTP := httptest.NewServer(http.HandlerFunc(second.ServeNodes))
	defer secondHTTP.Close()
	status := func(base string) int {
		req, err := http.NewRequest(http.MethodGet, base, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	firstCtx, stopFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Run(firstCtx) }()
	defer func() {
		stopFirst()
		<-firstDone
	}()
	eventually(t, func() bool { return status(firstHTTP.URL) == http.StatusOK })
	secondCtx, stopSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(secondCtx) }()
	defer func() {
		stopSecond()
		<-secondDone
	}()
	firstClient.Close()
	eventually(t, func() bool { return status(firstHTTP.URL) == http.StatusServiceUnavailable })
	stopFirst()
	eventually(t, func() bool { return status(secondHTTP.URL) == http.StatusOK })
}
