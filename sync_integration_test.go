package main

import (
	"context"
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
	httpServer := httptest.NewServer(master)
	defer httpServer.Close()
	follower, err := zonesync.NewFollower(followerDir, httpServer.URL, "secret", "1s")
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.Pull(context.Background()); err != nil {
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	syncDone := make(chan struct{})
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
	}()

	answerIP := func() string {
		t.Helper()
		recorder := dnstest.NewTestRecorder()
		dnsServer.ServeDNS(ctx, recorder, dns.NewMsg("www.example.com.", dns.TypeA))
		if recorder.Msg == nil {
			t.Fatal("DNS server sent no response")
		}
		if err := recorder.Msg.Unpack(); err != nil {
			t.Fatal(err)
		}
		if len(recorder.Msg.Answer) != 1 {
			t.Fatalf("expected one A record, got %d", len(recorder.Msg.Answer))
		}
		return recorder.Msg.Answer[0].(*dns.A).Addr.String()
	}
	if got := answerIP(); got != "192.0.2.1" {
		t.Fatalf("initial answer = %s", got)
	}

	write("192.0.2.2")
	deadline := time.Now().Add(5 * time.Second)
	for answerIP() != "192.0.2.2" {
		if time.Now().After(deadline) {
			t.Fatal("follower did not serve the updated A record")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
