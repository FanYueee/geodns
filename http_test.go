package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/abh/geodns/v3/appconfig"
	"github.com/abh/geodns/v3/targeting"
	"github.com/abh/geodns/v3/targeting/geoip2"
	"github.com/abh/geodns/v3/zones"
	"github.com/abh/geodns/v3/zonesync"
)

func TestHTTP(t *testing.T) {
	geoprovider, err := geoip2.New(geoip2.FindDB())
	if err == nil {
		targeting.Setup(geoprovider)
	}

	mm, err := zones.NewMuxManager("dns", &zones.NilReg{})
	if err != nil {
		t.Fatalf("loading zones: %s", err)
	}
	hs := NewHTTPServer(mm, serverInfo, nil)

	srv := httptest.NewServer(hs.Mux())

	baseurl := srv.URL
	t.Logf("server base url: '%s'", baseurl)

	// metrics := NewMetrics()
	// go metrics.Updater()

	res, err := http.Get(baseurl + "/version")
	require.Nil(t, err)
	page, _ := io.ReadAll(res.Body)

	if !bytes.HasPrefix(page, []byte("GeoDNS ")) {
		t.Log("/version didn't start with 'GeoDNS '")
		t.Fail()
	}
}

func TestSyncAPIUsesTokenWithHTTPBasicAuth(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "example.com.json"), []byte(`{"data":{"www":{"a":[["192.0.2.1",1]]}}}`), 0644); err != nil {
		t.Fatal(err)
	}
	master, err := zonesync.NewMaster(dir, "sync-secret")
	if err != nil {
		t.Fatal(err)
	}
	hs := NewHTTPServer(nil, serverInfo, master)
	handler := &basicauth{h: hs.Mux(), syncAPI: true}
	oldUser, oldPassword := appconfig.Config.HTTP.User, appconfig.Config.HTTP.Password
	appconfig.Config.HTTP.User, appconfig.Config.HTTP.Password = "metrics", "metrics-secret"
	t.Cleanup(func() {
		appconfig.Config.HTTP.User, appconfig.Config.HTTP.Password = oldUser, oldPassword
	})

	req := httptest.NewRequest(http.MethodGet, zonesync.Path, nil)
	req.Header.Set("Authorization", "Bearer sync-secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("sync API with token: HTTP %d", w.Code)
	}

	cluster, err := zonesync.NewCluster(&zonesync.Store{}, "pop-1", "sync-secret")
	if err != nil {
		t.Fatal(err)
	}
	apiHandler := &basicauth{h: NewHTTPServer(nil, serverInfo, cluster).Mux(), syncAPI: true}
	for _, path := range []string{zonesync.MasterPath, zonesync.Path + "/example.com"} {
		w = httptest.NewRecorder()
		apiHandler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusUnauthorized || !bytes.Contains(w.Body.Bytes(), []byte("unauthorized")) {
			t.Fatalf("management API bypassed token auth: %d %s", w.Code, w.Body)
		}
	}
	req = httptest.NewRequest(http.MethodGet, zonesync.Path+"/bad/name", nil)
	req.Header.Set("Authorization", "Bearer sync-secret")
	w = httptest.NewRecorder()
	apiHandler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("management bearer auth with HTTP basic configured: HTTP %d", w.Code)
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("metrics without basic auth: HTTP %d", w.Code)
	}
}
