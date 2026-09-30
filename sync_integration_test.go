package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	dns "codeberg.org/miekg/dns"
	"codeberg.org/miekg/dns/dnstest"
	"github.com/abh/geodns/v3/appconfig"
	"github.com/abh/geodns/v3/server"
	"github.com/abh/geodns/v3/zones"
	"github.com/abh/geodns/v3/zonesync"
	"github.com/prometheus/client_golang/prometheus"
)

func TestFollowerServesUpdatedMasterZone(t *testing.T) {
	masterDir := t.TempDir()
	followerDir := t.TempDir()
	zonePath := filepath.Join(masterDir, "example.com.json")
	write := func(ip string) {
		t.Helper()
		data := []byte(`{"data":{"www":{"a":[["` + ip + `",1]]}}}`)
		if err := os.WriteFile(zonePath, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("192.0.2.1")
	master, err := zonesync.NewMaster(masterDir, "secret")
	if err != nil {
		t.Fatal(err)
	}
	masterHTTP := http.NewServeMux()
	masterHTTP.HandleFunc(zonesync.StreamPath, master.ServeStream)
	masterHTTP.HandleFunc(zonesync.NodesPath, master.ServeNodes)
	httpServer := httptest.NewServer(masterHTTP)
	defer httpServer.Close()
	follower, err := zonesync.NewFollower(followerDir, httpServer.URL, "secret", "node-1")
	if err != nil {
		t.Fatal(err)
	}

	previousRegisterer := prometheus.DefaultRegisterer
	previousGatherer := prometheus.DefaultGatherer
	registry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = previousRegisterer
		prometheus.DefaultGatherer = previousGatherer
	})
	dnsServer := server.NewServer(appconfig.Config, serverInfo)
	mux, err := zones.NewMuxManager(followerDir, dnsServer)
	if err != nil {
		t.Fatal(err)
	}
	follower.SetReload(mux.Reload)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	syncDone := make(chan struct{})
	masterDone := make(chan error, 1)
	go func() { masterDone <- master.Run(ctx) }()
	go func() {
		mux.Run(ctx)
		close(done)
	}()
	go func() {
		follower.Run(ctx)
		close(syncDone)
	}()
	defer func() {
		cancel()
		<-done
		<-syncDone
		if err := <-masterDone; err != nil {
			t.Errorf("master watcher: %s", err)
		}
	}()

	answerIP := func() (string, error) {
		recorder := dnstest.NewTestRecorder()
		dnsServer.ServeDNS(ctx, recorder, dns.NewMsg("www.example.com.", dns.TypeA))
		if recorder.Msg == nil {
			return "", fmt.Errorf("DNS server sent no response")
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
		deadline := time.Now().Add(5 * time.Second)
		for {
			got, err := answerIP()
			if err == nil && got == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("DNS answer = %s, error = %v; want %s", got, err, want)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitAnswer("192.0.2.1")

	write("192.0.2.2")
	waitAnswer("192.0.2.2")
}
