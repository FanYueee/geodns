package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/abh/geodns/v3/appconfig"
	"go.etcd.io/etcd/server/v3/embed"
	"gopkg.in/gcfg.v1"
)

func TestEmbeddedConfigSampleAndValidation(t *testing.T) {
	var cfg appconfig.AppConfig
	if err := gcfg.ReadFileInto(&cfg, "dns/geodns.cluster.conf.sample"); err != nil {
		t.Fatal(err)
	}
	if _, node, err := embeddedConfig(cfg, "dns/geodns.conf"); err != nil || node == nil {
		t.Fatalf("embedded sample: %v", err)
	}
	cfg.Cluster.ZoneDirectory = "source-zones"
	cfg.Cluster.Weight = 300
	cfg.Cluster.ZoneMode = "api"
	resolved, node, err := embeddedConfig(cfg, "dns/geodns.conf")
	if err != nil {
		t.Fatal(err)
	}
	wantSource, _ := filepath.Abs("dns/source-zones")
	if resolved.Controller.ResolvePaths("dns/geodns.conf").ZoneDirectory != wantSource || !filepath.IsAbs(node.config.Dir) {
		t.Fatalf("relative paths resolved incorrectly: %+v %s", resolved.Controller, node.config.Dir)
	}
	if resolved.Controller.ZoneMode != "api" {
		t.Fatal("cluster zone mode was not passed to controller")
	}
	if resolved.Controller.Weight != 300 {
		t.Fatalf("cluster weight was not passed to controller: %d", resolved.Controller.Weight)
	}
	for _, tc := range []struct {
		name string
		edit func(*appconfig.AppConfig)
	}{
		{"wildcard", func(c *appconfig.AppConfig) { c.Cluster.Address = "0.0.0.0" }},
		{"id missing", func(c *appconfig.AppConfig) { c.Cluster.ID = "" }},
		{"wrong own address", func(c *appconfig.AppConfig) { c.Cluster.Address = "10.80.0.99" }},
		{"duplicate member", func(c *appconfig.AppConfig) { c.Cluster.Members += ",pop-1=10.80.0.14" }},
		{"duplicate address", func(c *appconfig.AppConfig) { c.Cluster.Members += ",pop-4=10.80.0.11" }},
		{"both join and bootstrap", func(c *appconfig.AppConfig) { c.Cluster.Join = "10.80.0.12" }},
		{"mixed legacy config", func(c *appconfig.AppConfig) { c.Sync.Mode = "ha" }},
		{"port conflict", func(c *appconfig.AppConfig) { c.Cluster.ClientPort = 2380 }},
		{"invalid name", func(c *appconfig.AppConfig) { c.Cluster.Name = "../other" }},
		{"invalid zone mode", func(c *appconfig.AppConfig) { c.Cluster.ZoneMode = "typo" }},
		{"negative weight", func(c *appconfig.AppConfig) { c.Cluster.Weight = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := cfg
			tc.edit(&copy)
			if _, _, err := embeddedConfig(copy, "dns/geodns.conf"); err == nil {
				t.Fatal("invalid embedded config accepted")
			}
		})
	}
	cfg.Cluster.Address = "fd00::1"
	cfg.Cluster.Members = "pop-1=fd00::1,pop-2=fd00::2,pop-3=fd00::3"
	_, node, err = embeddedConfig(cfg, "dns/geodns.conf")
	if err != nil || node.peerURL != "http://[fd00::1]:2380" {
		t.Fatalf("IPv6 config: %+v %v", node, err)
	}
	cfg.Cluster.Enabled = false
	cfg.Cluster.Address = "invalid-but-disabled"
	if _, node, err := embeddedConfig(cfg, "dns/geodns.conf"); err != nil || node != nil {
		t.Fatalf("disabled cluster did not retain standalone behavior: %+v %v", node, err)
	}
}

func reserveClusterPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func TestEmbeddedClusterSyncJoinRemoveFailoverAndRestart(t *testing.T) {
	clientPort, peerPort := reserveClusterPort(t), reserveClusterPort(t)
	for peerPort == clientPort {
		peerPort = reserveClusterPort(t)
	}
	addresses := []string{"127.0.0.11", "127.0.0.12", "127.0.0.13", "127.0.0.14"}
	members := "node-1=127.0.0.11,node-2=127.0.0.12,node-3=127.0.0.13"
	source := t.TempDir()
	zone := func(ip string) []byte { return []byte(fmt.Sprintf(`{"data":{"www":{"a":[["%s",1]]}}}`, ip)) }
	writeSource := func(ip string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(source, "example.com.json"), zone(ip), 0644); err != nil {
			t.Fatal(err)
		}
	}
	writeSource("192.0.2.1")
	type runningNode struct {
		cfg  appconfig.AppConfig
		dir  string
		url  string
		ha   *haNode
		stop func()
	}
	start := func(cfg appconfig.AppConfig, dir string) *runningNode {
		t.Helper()
		httpServer := httptest.NewUnstartedServer(nil)
		node, err := newHANode(cfg, dir, filepath.Join(dir, "geodns.conf"), httpServer.Listener.Addr().String(), true)
		if err != nil {
			httpServer.Close()
			t.Fatal(err)
		}
		// Faster local election tests; production keeps etcd's defaults.
		node.embedded.config.TickMs, node.embedded.config.ElectionMs = 50, 500
		node.embedded.config.LogLevel = "error"
		httpServer.Config.Handler = NewHTTPServer(nil, serverInfo, node.controller).Mux()
		httpServer.Start()
		ctx, cancel := context.WithCancel(context.Background())
		controllerDone, followerDone := make(chan error, 1), make(chan struct{})
		go func() { controllerDone <- node.Run(ctx) }()
		go func() { node.follower.Run(ctx); close(followerDone) }()
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			cancel()
			select {
			case err := <-controllerDone:
				if err != nil {
					t.Errorf("node %s stopped: %v", cfg.Cluster.ID, err)
				}
			case <-time.After(20 * time.Second):
				t.Errorf("node %s did not stop", cfg.Cluster.ID)
			}
			<-followerDone
			httpServer.Close()
			node.store.Close()
		}
		t.Cleanup(stop)
		return &runningNode{cfg: cfg, dir: dir, url: httpServer.URL, ha: node, stop: stop}
	}
	config := func(i int) appconfig.AppConfig {
		return appconfig.AppConfig{Cluster: appconfig.ClusterConfig{
			Enabled: true, ID: "node-" + strconv.Itoa(i+1), Address: addresses[i],
			Weight:  300 - i*100,
			Members: members, Token: "secret", Name: "embedded-test", ClientPort: clientPort, PeerPort: peerPort,
		}}
	}
	wait := func(check func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatal("embedded cluster did not reach expected state")
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	var nodes []*runningNode
	for i := 0; i < 3; i++ {
		cfg := config(i)
		if i == 0 {
			cfg.Cluster.ZoneDirectory = source
		}
		nodes = append(nodes, start(cfg, t.TempDir()))
	}
	waitSync := func(node *runningNode, ip string) {
		t.Helper()
		wait(func() bool {
			data, err := os.ReadFile(filepath.Join(node.dir, "example.com.json"))
			return err == nil && bytes.Equal(data, zone(ip))
		})
	}
	for _, node := range nodes {
		waitSync(node, "192.0.2.1")
	}
	client, err := nodes[0].ha.embedded.client()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	waitLocalReady := func(node *runningNode) {
		t.Helper()
		wait(func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			status, err := client.Status(ctx, node.ha.embedded.clientURL)
			return err == nil && status.Leader != 0 && !status.IsLearner
		})
	}
	writeSource("192.0.2.2")
	for _, node := range nodes {
		waitSync(node, "192.0.2.2")
	}
	var status bytes.Buffer
	if err := manageCluster(context.Background(), nodes[0].cfg, filepath.Join(nodes[0].dir, "geodns.conf"), "", &status); err != nil || !strings.Contains(status.String(), "GeoDNS controller: http://") {
		t.Fatalf("cluster status: %s %v", status.String(), err)
	}
	registered, err := client.MemberList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range registered.Members {
		if member.Name == nodes[0].cfg.Cluster.ID {
			if err := manageCluster(context.Background(), nodes[0].cfg, filepath.Join(nodes[0].dir, "geodns.conf"), fmt.Sprintf("%x", member.ID), &status); err == nil {
				t.Fatal("self-removal through hexadecimal member ID was accepted")
			}
		}
	}
	joinedCfg := config(3)
	joinedCfg.Cluster.Members, joinedCfg.Cluster.Join = "", strings.Join(addresses[:3], ",")
	_, pending, err := embeddedConfig(joinedCfg, filepath.Join(t.TempDir(), "geodns.conf"))
	if err != nil {
		t.Fatal(err)
	}
	var firstID uint64
	wait(func() bool {
		firstID, err = pending.register(context.Background())
		return err == nil
	})
	secondID, err := pending.register(context.Background())
	if err != nil || firstID != secondID {
		t.Fatalf("pending join was not idempotent: %x %x %v", firstID, secondID, err)
	}
	if err := manageCluster(context.Background(), nodes[0].cfg, filepath.Join(nodes[0].dir, "geodns.conf"), fmt.Sprintf("%x", firstID), &status); err != nil {
		t.Fatalf("remove unnamed learner: %v", err)
	}
	joined := start(joinedCfg, t.TempDir())
	waitSync(joined, "192.0.2.2")
	wait(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := client.MemberList(ctx)
		if err != nil || len(response.Members) != 4 {
			return false
		}
		for _, member := range response.Members {
			if member.IsLearner {
				return false
			}
		}
		return true
	})
	joined.stop()
	joined = start(joinedCfg, joined.dir)
	waitLocalReady(joined)
	waitSync(joined, "192.0.2.2")
	// A duplicate ID with a different address must not add a fifth member.
	duplicate := *joined.ha.embedded
	duplicate.config = embed.NewConfig()
	duplicate.config.Name, duplicate.peerURL = joinedCfg.Cluster.ID, "http://127.0.0.15:"+strconv.Itoa(peerPort)
	if _, err := duplicate.register(context.Background()); err == nil {
		t.Fatal("duplicate node ID joined the cluster")
	}
	joined.stop()
	if err := manageCluster(context.Background(), nodes[0].cfg, filepath.Join(nodes[0].dir, "geodns.conf"), "node-4", &status); err != nil {
		t.Fatal(err)
	}
	response, err := client.MemberList(context.Background())
	if err != nil || len(response.Members) != 3 {
		t.Fatalf("member removal failed: %v %v", response, err)
	}
	leaderURL, err := nodes[0].ha.store.LeaderURL(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	leaderIndex := -1
	for i, node := range nodes {
		if node.url == leaderURL {
			leaderIndex = i
		}
	}
	if leaderIndex < 0 {
		t.Fatalf("unknown leader %s", leaderURL)
	}
	if leaderIndex != 0 {
		t.Fatalf("highest-weight node did not lead: %s", leaderURL)
	}
	nodes[leaderIndex].stop()
	remaining := nodes[(leaderIndex+1)%3]
	wait(func() bool {
		url, err := remaining.ha.store.LeaderURL(context.Background())
		return err == nil && url == remaining.url
	})
	updates := t.TempDir()
	if err := os.WriteFile(filepath.Join(updates, "example.com.json"), zone("192.0.2.3"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := remaining.ha.store.Publish(context.Background(), updates); err != nil {
		t.Fatal(err)
	}
	for i, node := range nodes {
		if i != leaderIndex {
			waitSync(node, "192.0.2.3")
		}
	}
	// Restart the lost voter with its original data. No fresh member registration.
	restarted := start(nodes[leaderIndex].cfg, nodes[leaderIndex].dir)
	waitLocalReady(restarted)
	waitSync(restarted, "192.0.2.3")
	// Recovery reclaims controller leadership while keeping the original WAL.
	wait(func() bool {
		url, err := remaining.ha.store.LeaderURL(context.Background())
		return err == nil && url == restarted.url
	})
	writeSource("192.0.2.4")
	waitSync(restarted, "192.0.2.4")
	for i, node := range nodes {
		if i != leaderIndex {
			waitSync(node, "192.0.2.4")
		}
	}
	response, err = client.MemberList(context.Background())
	if err != nil || len(response.Members) != 3 {
		t.Fatalf("restart changed cluster membership: %v %v", response, err)
	}
}
