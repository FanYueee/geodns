package zonesync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestBootstrapNeverReplacesPublishedZones(t *testing.T) {
	store, err := NewStore(testEtcd(t), "/test/bootstrap")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	empty := t.TempDir()
	if _, err := store.Bootstrap(ctx, empty); err == nil {
		t.Fatal("empty bootstrap was accepted")
	}
	sources := []string{t.TempDir(), t.TempDir()}
	writeZone(t, sources[0], "example.com.json", testZone)
	writeZone(t, sources[1], "example.com.json", strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.2"))
	type result struct {
		revision string
		err      error
	}
	results := make(chan result, 2)
	for _, source := range sources {
		go func() {
			revision, err := store.Bootstrap(ctx, source)
			results <- result{revision, err}
		}()
	}
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.revision != second.revision {
		t.Fatalf("competing bootstraps disagree: %+v, %+v", first, second)
	}
	updatedDir := t.TempDir()
	writeZone(t, updatedDir, "example.com.json", strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.3"))
	updated, err := store.Publish(ctx, updatedDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range append(sources, filepath.Join(t.TempDir(), "missing")) {
		got, err := store.Bootstrap(ctx, source)
		if err != nil || got != updated {
			t.Fatalf("restart bootstrap replaced latest revision: %s, %v", got, err)
		}
	}
	snapshot, _, err := store.loadCurrent(ctx)
	if err != nil || !strings.Contains(string(snapshot.Zones["example.com.json"]), "192.0.2.3") {
		t.Fatalf("bootstrap corrupted published zones: %v", err)
	}
}

func TestBootstrapRejectsInvalidZones(t *testing.T) {
	store, err := NewStore(testEtcd(t), "/test/invalid-bootstrap")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeZone(t, dir, "example.com.json", `{"data":{},"targeting":"invalid-target"}`)
	if _, err := store.Bootstrap(context.Background(), dir); err == nil {
		t.Fatal("invalid zone bootstrapped")
	}
	if _, _, err := store.currentMetadata(context.Background()); err != ErrNoSnapshot {
		t.Fatalf("invalid bootstrap changed current snapshot: %v", err)
	}
}

func TestDiscoveryAddressValidation(t *testing.T) {
	cluster, err := NewCluster(&Store{}, "node-1", "secret")
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"http://:8053", "http://0.0.0.0:8053", "http://[::]:8053", "file:///tmp/key", "http://user:secret@example.com", "http://example.com/api"} {
		if err := cluster.SetAdvertiseURL(address); err == nil {
			t.Errorf("invalid advertise address accepted: %s", address)
		}
	}
	if err := cluster.SetAdvertiseURL("http://10.80.0.11:8053"); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryDoesNotSelectStandbyAddress(t *testing.T) {
	client := testEtcd(t)
	store, err := NewStore(client, "/test/discovery")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.LeaderURL(ctx); err == nil {
		t.Fatal("empty election returned a leader")
	}
	// Legacy leaders cannot be discovered; a later advertised candidate is not the leader.
	if _, err := client.Put(ctx, store.prefix+"/election/first", "legacy-controller"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(ctx, store.prefix+"/election/second", `{"id":"node-2","url":"http://127.0.0.1:8053"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LeaderURL(ctx); err == nil {
		t.Fatal("standby candidate was returned instead of legacy leader")
	}
	if _, err := client.Delete(ctx, store.prefix+"/election/first"); err != nil {
		t.Fatal(err)
	}
	got, err := store.LeaderURL(ctx)
	if err != nil || got != "http://127.0.0.1:8053" {
		t.Fatalf("new leader discovery: %q, %v", got, err)
	}
	if _, err := client.Delete(ctx, store.prefix+"/election/", clientv3.WithPrefix()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LeaderURL(ctx); err == nil {
		t.Fatal("removed controller was still discovered")
	}
}

func followerHasIP(dir, ip string) bool {
	data, err := os.ReadFile(filepath.Join(dir, "example.com.json"))
	return err == nil && strings.Contains(string(data), ip)
}

func TestDiscoveryJoinsAndFailsOverWithoutURLLists(t *testing.T) {
	client := testEtcd(t)
	store, err := NewStore(client, "/test/dynamic-nodes")
	if err != nil {
		t.Fatal(err)
	}
	source := t.TempDir()
	writeZone(t, source, "example.com.json", testZone)
	if _, err := store.Bootstrap(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	type node struct {
		url  string
		dir  string
		stop func()
	}
	startNode := func(id string) node {
		t.Helper()
		cluster, err := NewCluster(store, id, "secret")
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc(StreamPath, cluster.ServeStream)
		httpServer := httptest.NewServer(mux)
		t.Cleanup(httpServer.Close)
		if err := cluster.SetAdvertiseURL(httpServer.URL); err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		follower, err := NewDiscoveredFollower(dir, "secret", id, store)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		controllerDone, followerDone := make(chan error, 1), make(chan struct{})
		go func() { controllerDone <- cluster.Run(ctx) }()
		go func() {
			follower.Run(ctx)
			close(followerDone)
		}()
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			cancel()
			<-followerDone
			if err := <-controllerDone; err != nil {
				t.Errorf("controller %s stopped: %s", id, err)
			}
		}
		t.Cleanup(stop)
		return node{url: httpServer.URL, dir: dir, stop: stop}
	}
	waitLeader := func(want string) {
		t.Helper()
		eventually(t, func() bool {
			got, err := store.LeaderURL(context.Background())
			return err == nil && got == want
		})
	}
	first := startNode("node-1")
	waitLeader(first.url)
	eventually(t, func() bool { return followerHasIP(first.dir, "192.0.2.1") })
	second := startNode("node-2")
	eventually(t, func() bool { return followerHasIP(second.dir, "192.0.2.1") })
	first.stop()
	waitLeader(second.url)
	writeZone(t, source, "example.com.json", strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.2"))
	if _, err := store.Publish(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return followerHasIP(second.dir, "192.0.2.2") })
	// A new node joins the existing election and obtains the latest zones without editing old configs.
	third := startNode("node-3")
	eventually(t, func() bool { return followerHasIP(third.dir, "192.0.2.2") })
	second.stop()
	waitLeader(third.url)
	writeZone(t, source, "example.com.json", strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.3"))
	if _, err := store.Publish(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return followerHasIP(third.dir, "192.0.2.3") })
	third.stop()
	eventually(t, func() bool {
		_, err := store.LeaderURL(context.Background())
		return err != nil
	})
}
