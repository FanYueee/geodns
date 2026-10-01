package zonesync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestClusterWeightPreemptionRecoveryAndEqualWeight(t *testing.T) {
	store, err := NewStore(testEtcd(t), "/test/priority")
	if err != nil {
		t.Fatal(err)
	}
	source, cache := t.TempDir(), t.TempDir()
	writeZone(t, source, "example.com.json", testZone)
	if _, err := store.Publish(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	type node struct {
		cluster *Cluster
		server  *httptest.Server
		stop    func()
	}
	newNode := func(id string, weight int) *node {
		t.Helper()
		cluster, err := NewCluster(store, id, "secret")
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.SetWeight(weight); err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc(StreamPath, cluster.ServeStream)
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)
		if err := cluster.SetAdvertiseURL(server.URL); err != nil {
			t.Fatal(err)
		}
		return &node{cluster: cluster, server: server}
	}
	start := func(n *node) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- n.cluster.Run(ctx) }()
		stopped := false
		n.stop = func() {
			if stopped {
				return
			}
			stopped = true
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("controller stopped: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("controller shutdown timed out")
			}
		}
		t.Cleanup(n.stop)
	}
	waitLeader := func(n *node) {
		t.Helper()
		eventually(t, func() bool {
			url, err := store.LeaderURL(context.Background())
			n.cluster.mu.RLock()
			ready := n.cluster.master != nil
			n.cluster.mu.RUnlock()
			return err == nil && url == n.server.URL && ready
		})
	}
	low, medium, high, equal := newNode("low", 100), newNode("medium", 200), newNode("high", 300), newNode("equal", 300)
	start(low)
	waitLeader(low)
	follower, err := NewDiscoveredFollower(cache, "secret", "dns-node", store)
	if err != nil {
		t.Fatal(err)
	}
	followerCtx, stopFollower := context.WithCancel(context.Background())
	followerDone := make(chan struct{})
	go func() { follower.Run(followerCtx); close(followerDone) }()
	t.Cleanup(func() { stopFollower(); <-followerDone })
	eventually(t, func() bool { return followerHasIP(cache, "192.0.2.1") })
	start(medium)
	waitLeader(medium)
	start(high)
	waitLeader(high)
	start(equal)
	// An equally weighted newcomer must register without displacing the leader.
	eventually(t, func() bool {
		resp, err := store.client.Get(context.Background(), store.prefix+"/candidates/", clientv3.WithPrefix())
		return err == nil && len(resp.Kvs) == 4
	})
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		url, err := store.LeaderURL(context.Background())
		if err != nil || url != high.server.URL {
			t.Fatalf("equal weight displaced leader: %s %v", url, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	equal.stop()
	high.stop()
	waitLeader(medium)
	medium.stop()
	waitLeader(low)
	start(medium)
	waitLeader(medium)
	start(high)
	waitLeader(high)
	writeZone(t, source, "example.com.json", strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.2"))
	if _, err := store.Publish(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	// The restored leader serves the newest snapshot and pushes live edits after
	// the follower reconnects, without restarting its DNS process.
	eventually(t, func() bool { return followerHasIP(cache, "192.0.2.2") })
}

func TestClusterRejectsNegativeWeight(t *testing.T) {
	cluster, err := NewCluster(&Store{}, "node", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.SetWeight(-1); err == nil {
		t.Fatal("negative controller weight accepted")
	}
}
