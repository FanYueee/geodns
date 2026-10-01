package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	dns "codeberg.org/miekg/dns"
	"codeberg.org/miekg/dns/dnstest"
	"github.com/abh/geodns/v3/appconfig"
	"github.com/abh/geodns/v3/server"
	"github.com/abh/geodns/v3/zones"
	"github.com/abh/geodns/v3/zonesync"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/server/v3/embed"
	"gopkg.in/gcfg.v1"
)

func TestHANodeConfigSample(t *testing.T) {
	var cfg appconfig.AppConfig
	if err := gcfg.ReadFileInto(&cfg, "dns/geodns.ha-node.conf.sample"); err != nil {
		t.Fatal(err)
	}
	if cfg.Sync.Mode != "ha" || cfg.Sync.ID == "" || cfg.Sync.URLs != "" || cfg.Sync.Token == "" || cfg.Controller.Listen == "" || cfg.Controller.EtcdEndpoints == "" {
		t.Fatalf("incomplete HA node sample: sync=%+v controller=%+v", cfg.Sync, cfg.Controller)
	}
	if _, err := publishHANode(context.Background(), cfg, "dns/geodns.ha-node.conf.sample"); err == nil || !strings.Contains(err.Error(), "zone-directory") {
		t.Fatalf("publish without source directory: %v", err)
	}
}

func TestHANodeKeepsAuthoritativeZonesSeparate(t *testing.T) {
	dir := t.TempDir()
	cfg := appconfig.AppConfig{}
	cfg.Sync.Mode = "ha"
	cfg.Sync.URLs = "http://127.0.0.1:8053"
	cfg.Controller.Listen = "127.0.0.1:8053"
	cfg.Controller.ZoneDirectory = dir
	if _, err := newHANode(cfg, dir, filepath.Join(dir, "geodns.conf"), ":8053", false); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("shared source and cache directory: %v", err)
	}
}

func TestHANodeDiscoveryAndLegacyConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		listen    string
		advertise string
		urls      string
		wantError bool
	}{
		{name: "automatic", listen: "127.0.0.1:8053"},
		{name: "wildcard needs address", listen: ":8053", wantError: true},
		{name: "wildcard with proxy", listen: ":8053", advertise: "https://node.example.com"},
		{name: "legacy list", listen: ":8053", urls: "http://127.0.0.1:8053"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := appconfig.AppConfig{}
			cfg.Sync.Mode, cfg.Sync.ID, cfg.Sync.Token = "ha", "node-1", "secret"
			cfg.Sync.URLs = tc.urls
			cfg.Controller.Listen, cfg.Controller.Advertise = tc.listen, tc.advertise
			cfg.Controller.EtcdEndpoints = "http://127.0.0.1:2379"
			dir := t.TempDir()
			node, err := newHANode(cfg, dir, filepath.Join(dir, "geodns.conf"), ":8053", false)
			if tc.wantError {
				if err == nil {
					node.store.Close()
					t.Fatal("invalid listen address accepted without an advertise address")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer node.store.Close()
		})
	}
}

func TestHANodeServesAndReceivesPublishedZones(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		t.Run(fmt.Sprintf("embedded=%t", embedded), func(t *testing.T) {
			testHANodeServesAndReceivesZones(t, embedded)
		})
	}
}

func testHANodeServesAndReceivesZones(t *testing.T, embedded bool) {
	reserveURL := func() url.URL {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := listener.Addr().String()
		listener.Close()
		return url.URL{Scheme: "http", Host: addr}
	}
	etcdConfig := embed.NewConfig()
	etcdConfig.Dir = t.TempDir()
	etcdConfig.LogLevel = "error"
	etcdConfig.LogOutputs = []string{os.DevNull}
	peerURL, clientURL := reserveURL(), reserveURL()
	etcdConfig.ListenPeerUrls = []url.URL{peerURL}
	etcdConfig.AdvertisePeerUrls = []url.URL{peerURL}
	etcdConfig.ListenClientUrls = []url.URL{clientURL}
	etcdConfig.AdvertiseClientUrls = []url.URL{clientURL}
	etcdConfig.InitialCluster = etcdConfig.InitialClusterFromName(etcdConfig.Name)
	if !embedded {
		etcd, err := embed.StartEtcd(etcdConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer etcd.Close()
		select {
		case <-etcd.Server.ReadyNotify():
		case <-time.After(15 * time.Second):
			t.Fatal("external etcd did not start")
		}
	}

	sourceDir, nodeDir := t.TempDir(), t.TempDir()
	zone := []byte(`{"data":{"www":{"a":[["192.0.2.1",1]]}}}`)
	if err := os.WriteFile(filepath.Join(sourceDir, "example.com.json"), zone, 0644); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewUnstartedServer(nil)
	defer httpServer.Close()
	cfg := appconfig.AppConfig{}
	cfg.Sync.Mode = "ha"
	cfg.Sync.ID = "node-1"
	cfg.Sync.Token = "secret"
	cfg.Controller.Mode = "ha"
	cfg.Controller.Listen = httpServer.Listener.Addr().String()
	cfg.Controller.EtcdEndpoints = clientURL.String()
	cfg.Controller.EtcdPrefix = "/test/ha-node"
	cfg.Controller.ZoneDirectory = sourceDir
	if embedded {
		cfg.Sync, cfg.Controller = appconfig.SyncConfig{}, appconfig.ControllerConfig{}
		clientPort, _ := strconv.Atoi(clientURL.Port())
		peerPort, _ := strconv.Atoi(peerURL.Port())
		_, syncPortText, _ := net.SplitHostPort(httpServer.Listener.Addr().String())
		syncPort, _ := strconv.Atoi(syncPortText)
		cfg.Cluster = appconfig.ClusterConfig{
			Enabled: true, ID: "node-1", Address: "127.0.0.1", Token: "secret",
			Members: "node-1=127.0.0.1", ZoneDirectory: sourceDir,
			ClientPort: clientPort, PeerPort: peerPort, SyncPort: syncPort,
		}
	}
	node, err := newHANode(cfg, nodeDir, filepath.Join(nodeDir, "geodns.conf"), ":8053", false)
	if err != nil {
		t.Fatal(err)
	}
	defer node.store.Close()
	if node.httpAddr != httpServer.Listener.Addr().String() {
		t.Fatalf("HTTP listen address = %q, want %q", node.httpAddr, cfg.Controller.Listen)
	}
	previousRegisterer, previousGatherer := prometheus.DefaultRegisterer, prometheus.DefaultGatherer
	registry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer, prometheus.DefaultGatherer = registry, registry
	t.Cleanup(func() {
		prometheus.DefaultRegisterer, prometheus.DefaultGatherer = previousRegisterer, previousGatherer
	})
	dnsServer := server.NewServer(&cfg, serverInfo)
	mux, err := zones.NewMuxManager(nodeDir, dnsServer)
	if err != nil {
		t.Fatal(err)
	}
	node.follower.SetReload(mux.Reload)
	handler := NewHTTPServer(nil, serverInfo, node.controller)
	httpServer.Config.Handler = handler.Mux()
	httpServer.Start()
	ctx, cancel := context.WithCancel(context.Background())
	controllerDone := make(chan error, 1)
	followerDone := make(chan struct{})
	muxDone := make(chan struct{})
	go func() { controllerDone <- node.Run(ctx) }()
	go func() {
		mux.Run(ctx)
		close(muxDone)
	}()
	go func() {
		node.follower.Run(ctx)
		close(followerDone)
	}()
	defer func() {
		cancel()
		<-muxDone
		<-followerDone
		if err := <-controllerDone; err != nil {
			t.Errorf("controller stopped: %s", err)
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(filepath.Join(nodeDir, "example.com.json"))
		if err == nil && string(data) == string(zone) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("co-located follower did not receive zone: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	req, err := http.NewRequest(http.MethodGet, httpServer.URL+zonesync.NodesPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leader node status endpoint: HTTP %d", resp.StatusCode)
	}
	var statuses []zonesync.NodeStatus
	if err := json.NewDecoder(resp.Body).Decode(&statuses); err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || statuses[0].ID != "node-1" || !statuses[0].Connected {
		t.Fatalf("leader did not track co-located follower: %+v", statuses)
	}
	answerIP := func() (string, error) {
		recorder := dnstest.NewTestRecorder()
		dnsServer.ServeDNS(ctx, recorder, dns.NewMsg("www.example.com.", dns.TypeA))
		if recorder.Msg == nil {
			return "", errors.New("DNS server sent no response")
		}
		if err := recorder.Msg.Unpack(); err != nil {
			return "", err
		}
		if len(recorder.Msg.Answer) != 1 {
			return "", fmt.Errorf("expected one A record, got %d", len(recorder.Msg.Answer))
		}
		return recorder.Msg.Answer[0].(*dns.A).Addr.String(), nil
	}
	waitAnswer := func(want string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			got, err := answerIP()
			if err == nil && got == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("DNS answer = %s, %v; want %s", got, err, want)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitAnswer("192.0.2.1")
	updated := []byte(`{"data":{"www":{"a":[["192.0.2.2",1]]}}}`)
	if err := os.WriteFile(filepath.Join(sourceDir, "example.com.json"), updated, 0644); err != nil {
		t.Fatal(err)
	}
	waitAnswer("192.0.2.2")
}
