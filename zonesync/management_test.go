package zonesync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type managementNode struct {
	cluster *Cluster
	url     string
	stop    func()
}

func startManagementNode(t *testing.T, store *Store, id string, weight int) managementNode {
	t.Helper()
	c, err := NewCluster(store, id, "secret")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetZoneMode("api"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetWeight(weight); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	RegisterManagementRoutes(mux, c)
	mux.Handle(Path, c)
	mux.HandleFunc(StreamPath, c.ServeStream)
	mux.HandleFunc(NodesPath, c.ServeNodes)
	srv := httptest.NewServer(mux)
	if err := c.SetAdvertiseURL(srv.URL); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil {
			t.Errorf("controller %s: %v", id, err)
		}
	}
	t.Cleanup(srv.Close)
	t.Cleanup(stop)
	return managementNode{c, srv.URL, stop}
}

func managementRequest(t *testing.T, base, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r, err := http.NewRequest(method, base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer secret")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	w := httptest.NewRecorder()
	w.HeaderMap = resp.Header.Clone()
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		t.Fatal(err)
	}
	return w
}

func expectManagementCode(t *testing.T, response *httptest.ResponseRecorder, code int) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("HTTP %d, want %d: %s", response.Code, code, response.Body)
	}
}

func TestManagementAPIUpdatesFailoverAndVersions(t *testing.T) {
	store, err := NewStore(testEtcd(t), "/test/management")
	if err != nil {
		t.Fatal(err)
	}
	first := startManagementNode(t, store, "pop-1", 300)
	eventually(t, first.cluster.IsLeader)
	second := startManagementNode(t, store, "pop-2", 200)
	discovery := managementRequest(t, second.url, "GET", MasterPath, "", nil)
	expectManagementCode(t, discovery, 200)
	var master MasterInfo
	if err := json.Unmarshal(discovery.Body.Bytes(), &master); err != nil {
		t.Fatal(err)
	}
	if master.ID != "pop-1" || master.URL != first.url || master.Term == 0 {
		t.Fatalf("master: %+v", master)
	}

	create := map[string]string{"If-None-Match": "*"}
	zonePath := Path + "/example.com"
	wrong := managementRequest(t, second.url, "PUT", zonePath, testZone, create)
	expectManagementCode(t, wrong, 409)
	if !strings.Contains(wrong.Body.String(), first.url) {
		t.Fatal("standby did not supply master discovery information")
	}
	if _, _, err := store.loadCurrent(context.Background()); !errors.Is(err, ErrNoSnapshot) {
		t.Fatal("standby changed zones")
	}

	response := managementRequest(t, first.url, "PUT", zonePath, testZone, create)
	expectManagementCode(t, response, 201)
	initialTag := response.Header().Get("ETag")
	if initialTag == "" {
		t.Fatal("missing write version")
	}
	get := managementRequest(t, first.url, "GET", zonePath, "", nil)
	expectManagementCode(t, get, 200)
	if get.Header().Get("ETag") != initialTag || !strings.Contains(get.Body.String(), "192.0.2.1") {
		t.Fatal("read did not match committed data")
	}
	expectManagementCode(t, managementRequest(t, first.url, "PUT", zonePath, testZone, create), 412)
	expectManagementCode(t, managementRequest(t, first.url, "PUT", zonePath, testZone, nil), 428)
	expectManagementCode(t, managementRequest(t, first.url, "GET", Path+"/missing.example", "", nil), 404)

	// A -> B -> A has the same content revision but a newer concurrency version.
	aba := managementRequest(t, first.url, "PUT", zonePath, strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.8"), map[string]string{"If-Match": initialTag})
	expectManagementCode(t, aba, 200)
	restored := managementRequest(t, first.url, "PUT", zonePath, testZone, map[string]string{"If-Match": aba.Header().Get("ETag")})
	expectManagementCode(t, restored, 200)
	if restored.Header().Get("X-Zone-Revision") != response.Header().Get("X-Zone-Revision") || restored.Header().Get("ETag") == initialTag {
		t.Fatal("version does not protect against stale A -> B -> A edits")
	}
	expectManagementCode(t, managementRequest(t, first.url, "PUT", zonePath, testZone, map[string]string{"If-Match": initialTag}), 412)
	expectManagementCode(t, managementRequest(t, second.url, "GET", Path, "", nil), 409)
	export := managementRequest(t, first.url, "GET", Path, "", nil)
	expectManagementCode(t, export, 200)
	var exported snapshot
	if err := json.Unmarshal(export.Body.Bytes(), &exported); err != nil || len(exported.Zones) != 1 {
		t.Fatalf("invalid export: %v", err)
	}

	// A discovered follower consumes API commits over the existing push stream.
	followerDir := t.TempDir()
	follower, err := NewDiscoveredFollower(followerDir, "secret", "dns-node", store)
	if err != nil {
		t.Fatal(err)
	}
	fctx, stopFollower := context.WithCancel(context.Background())
	fDone := make(chan struct{})
	go func() { follower.Run(fctx); close(fDone) }()
	defer func() { stopFollower(); <-fDone }()
	eventually(t, func() bool { return followerHasIP(followerDir, "192.0.2.1") })

	// The ETag covers the full set; independent additions preserve existing Zones.
	expectManagementCode(t, managementRequest(t, first.url, "PUT", Path+"/other.example", testZone, create), 201)
	stale := managementRequest(t, first.url, "PUT", zonePath, testZone, map[string]string{"If-Match": initialTag})
	expectManagementCode(t, stale, 412)
	get = managementRequest(t, first.url, "GET", zonePath, "", nil)
	currentTag := get.Header().Get("ETag")
	updated := strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.2")
	// Concurrent editors using the same version cannot both succeed.
	results := make(chan *httptest.ResponseRecorder, 2)
	for _, body := range []string{updated, strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.3")} {
		go func() {
			results <- managementRequest(t, first.url, "PUT", zonePath, body, map[string]string{"If-Match": currentTag})
		}()
	}
	a, b := <-results, <-results
	if !((a.Code == 200 && b.Code == 412) || (a.Code == 412 && b.Code == 200)) {
		t.Fatalf("concurrent edits: %d, %d", a.Code, b.Code)
	}
	get = managementRequest(t, first.url, "GET", zonePath, "", nil)
	currentTag = get.Header().Get("ETag")
	updatedResponse := managementRequest(t, first.url, "PUT", zonePath, updated, map[string]string{"If-Match": currentTag})
	expectManagementCode(t, updatedResponse, 200)
	eventually(t, func() bool { return followerHasIP(followerDir, "192.0.2.2") })
	expectManagementCode(t, managementRequest(t, first.url, "GET", Path+"/other.example", "", nil), 200)

	// Neither stale local files nor manual publication can overwrite API data.
	source := t.TempDir()
	writeZone(t, source, "example.com.json", testZone)
	if _, err := store.Publish(context.Background(), source); !errors.Is(err, errAPIMode) {
		t.Fatalf("legacy publication: %v", err)
	}
	if _, err := store.Bootstrap(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if err := store.ImportSource(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	expectManagementCode(t, managementRequest(t, first.url, "GET", zonePath, "", nil), 200)
	if !followerHasIP(followerDir, "192.0.2.2") {
		t.Fatal("restart import replaced API data")
	}

	first.stop()
	eventually(t, second.cluster.IsLeader)
	oldMaster := managementRequest(t, first.url, "DELETE", zonePath, "", map[string]string{"If-Match": updatedResponse.Header().Get("ETag")})
	expectManagementCode(t, oldMaster, 409)
	discovery = managementRequest(t, first.url, "GET", MasterPath, "", nil)
	expectManagementCode(t, discovery, 200)
	if !strings.Contains(discovery.Body.String(), second.url) {
		t.Fatal("old node cannot discover new master")
	}
	get = managementRequest(t, second.url, "GET", zonePath, "", nil)
	expectManagementCode(t, get, 200)
	finalData := strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.4")
	expectManagementCode(t, managementRequest(t, second.url, "PUT", zonePath, finalData, map[string]string{"If-Match": get.Header().Get("ETag")}), 200)
	eventually(t, func() bool { return followerHasIP(followerDir, "192.0.2.4") })
	// Higher-weight recovery resumes master service with committed API data.
	recovered := startManagementNode(t, store, "pop-1", 300)
	eventually(t, recovered.cluster.IsLeader)
	expectManagementCode(t, managementRequest(t, second.url, "GET", zonePath, "", nil), 409)
	get = managementRequest(t, recovered.url, "GET", zonePath, "", nil)
	expectManagementCode(t, get, 200)
	if !strings.Contains(get.Body.String(), "192.0.2.4") {
		t.Fatal("recovered master used stale Zone data")
	}
	expectManagementCode(t, managementRequest(t, recovered.url, "DELETE", zonePath, "", map[string]string{"If-Match": get.Header().Get("ETag")}), 200)
	recovered.stop()
	eventually(t, second.cluster.IsLeader)
	expectManagementCode(t, managementRequest(t, second.url, "GET", zonePath, "", nil), 404)
	eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(followerDir, "example.com.json"))
		return os.IsNotExist(err)
	})
	expectManagementCode(t, managementRequest(t, second.url, "GET", Path+"/other.example", "", nil), 200)
	other := managementRequest(t, second.url, "GET", Path+"/other.example", "", nil)
	expectManagementCode(t, managementRequest(t, second.url, "DELETE", Path+"/other.example", "", map[string]string{"If-Match": other.Header().Get("ETag")}), 200)
	if _, err := store.Bootstrap(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	empty := managementRequest(t, second.url, "GET", Path, "", nil)
	expectManagementCode(t, empty, 200)
	var emptySet snapshot
	if err := json.Unmarshal(empty.Body.Bytes(), &emptySet); err != nil || len(emptySet.Zones) != 0 {
		t.Fatal("deleted Zones were reimported")
	}
	if err := store.configureZoneMode(context.Background(), "files"); !errors.Is(err, errAPIMode) {
		t.Fatalf("stale file-mode restart accepted: %v", err)
	}
	second.stop()
	expectManagementCode(t, managementRequest(t, first.url, "GET", MasterPath, "", nil), 503)
}

func TestManagementRejectsInvalidRequests(t *testing.T) {
	store, err := NewStore(testEtcd(t), "/test/management-validation")
	if err != nil {
		t.Fatal(err)
	}
	node := startManagementNode(t, store, "pop-1", 100)
	eventually(t, node.cluster.IsLeader)
	for _, tc := range []struct {
		method, path, body string
		headers            map[string]string
		code               int
	}{
		{"GET", MasterPath, "", map[string]string{"Authorization": "Bearer wrong"}, 401},
		{"POST", MasterPath, "", nil, 405},
		{"POST", Path + "/example.com", "", nil, 405},
		{"PUT", Path + "/example.com", "{", map[string]string{"If-None-Match": "*"}, 400},
		{"PUT", Path + "/example.com", "null", map[string]string{"If-None-Match": "*"}, 400},
		{"PUT", Path + "/example.com", `{"data":{"www":{"a":[["invalid-ip",1]]}}}`, map[string]string{"If-None-Match": "*"}, 400},
		{"PUT", Path + "/example.com", `{"data":{},"targeting":"bad-target"}`, map[string]string{"If-None-Match": "*"}, 400},
		{"PUT", Path + "/example.com", strings.Repeat(" ", maxZoneSize+1), map[string]string{"If-None-Match": "*"}, 413},
		{"PUT", Path + "/bad..example", testZone, map[string]string{"If-None-Match": "*"}, 400},
		{"PUT", Path + "/" + strings.Repeat("a", 64) + ".example", testZone, map[string]string{"If-None-Match": "*"}, 400},
		{"PUT", Path + "/nested/example.com", testZone, map[string]string{"If-None-Match": "*"}, 400},
		{"PUT", Path + "/example.com", testZone, map[string]string{"If-None-Match": "*", "X-Master-Term": "0"}, 409},
	} {
		expectManagementCode(t, managementRequest(t, node.url, tc.method, tc.path, tc.body, tc.headers), tc.code)
	}
	if _, _, err := store.loadCurrent(context.Background()); !errors.Is(err, ErrNoSnapshot) {
		t.Fatal("rejected requests changed data")
	}
}

func TestManagementCommitFencesOldTerm(t *testing.T) {
	store, err := NewStore(testEtcd(t), "/test/management-fence")
	if err != nil {
		t.Fatal(err)
	}
	node := startManagementNode(t, store, "pop-1", 100)
	eventually(t, node.cluster.IsLeader)
	response := managementRequest(t, node.url, "PUT", Path+"/example.com", testZone, map[string]string{"If-None-Match": "*"})
	expectManagementCode(t, response, 201)
	node.cluster.mu.RLock()
	l := node.cluster.leadership
	node.cluster.mu.RUnlock()
	ctx := context.Background()
	mutex, release, err := store.lockPublication(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	snap, _, err := store.loadManaged(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	previous := snap.Revision
	snap.Zones["example.com.json"] = json.RawMessage(strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.9"))
	snap.Revision, _ = revisionOfZones(snap.Zones)
	// Replace the election key with the SAME lease, but a different create term.
	// This reproduces handoff/re-election after an in-flight request loaded data.
	if _, err := store.client.Delete(ctx, l.key); err != nil {
		t.Fatal(err)
	}
	if _, err := store.client.Put(ctx, l.key, `{"id":"replacement","url":"http://127.0.0.1:8053"}`, clientv3.WithLease(l.lease)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.commitSnapshot(ctx, mutex, snap, previous, l.guards()); !errors.Is(err, errLeadershipChanged) {
		t.Fatalf("old term committed: %v", err)
	}
	current, _, err := store.loadCurrent(ctx)
	if err != nil || current.Revision != previous {
		t.Fatalf("failed fencing changed published snapshot: %v", err)
	}
}
