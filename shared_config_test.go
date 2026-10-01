package main

import (
	"testing"

	"github.com/abh/geodns/v3/appconfig"
	"gopkg.in/gcfg.v1"
)

func TestSharedConfigAutoIdentifiesLocalNodeAndDNSAddresses(t *testing.T) {
	var cfg appconfig.AppConfig
	if err := gcfg.ReadStringInto(&cfg, `[cluster]
enabled = true
token = secret
bootstrap = local
[node "local"]
address = 127.0.0.1
listen = 192.0.2.1,2001:db8::1
weight = 300
zone-directory = source-zones
[node "remote"]
address = 192.0.2.2
listen = 192.0.2.22
weight = 200
`); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveSharedConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Cluster.ID != "local" || resolved.Cluster.Address != "127.0.0.1" || resolved.Cluster.Weight != 300 || resolved.Cluster.ZoneDirectory != "source-zones" || resolved.DNS.Listen != "192.0.2.1,2001:db8::1" {
		t.Fatalf("wrong local settings: %+v", resolved)
	}
	if _, node, err := embeddedConfig(cfg, "dns/geodns.conf"); err != nil || !node.shared || !node.bootstrap {
		t.Fatalf("shared bootstrap config: %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*appconfig.AppConfig)
	}{
		{"missing bootstrap", func(c *appconfig.AppConfig) { c.Cluster.Bootstrap = "missing" }},
		{"missing local node", func(c *appconfig.AppConfig) { c.Node["local"].Address = "192.0.2.99" }},
		{"duplicate address", func(c *appconfig.AppConfig) { c.Node["remote"].Address = "127.0.0.1" }},
		{"negative weight", func(c *appconfig.AppConfig) { c.Node["remote"].Weight = -1 }},
		{"invalid DNS IP", func(c *appconfig.AppConfig) { c.Node["remote"].Listen = "*" }},
		{"no DNS IP", func(c *appconfig.AppConfig) { c.Node["remote"].Listen = "" }},
		{"legacy members", func(c *appconfig.AppConfig) { c.Cluster.Members = "local=127.0.0.1" }},
		{"legacy join", func(c *appconfig.AppConfig) { c.Cluster.Join = "192.0.2.2" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := cfg
			copy.Node = map[string]*appconfig.ClusterNodeConfig{}
			for id, node := range cfg.Node {
				value := *node
				copy.Node[id] = &value
			}
			tc.edit(&copy)
			if _, err := resolveSharedConfig(copy); err == nil {
				t.Fatal("invalid shared config accepted")
			}
		})
	}
	cfg.Cluster.Enabled = false
	cfg.Node["local"].Address = "invalid"
	if _, err := resolveSharedConfig(cfg); err != nil {
		t.Fatalf("disabled cluster: %v", err)
	}
}

func TestSharedConfigSample(t *testing.T) {
	var cfg appconfig.AppConfig
	if err := gcfg.ReadFileInto(&cfg, "dns/geodns.shared.conf.sample"); err != nil {
		t.Fatal(err)
	}
	cfg.Cluster.ID = "pop-2"
	resolved, node, err := embeddedConfig(cfg, "dns/geodns.conf")
	if err != nil {
		t.Fatal(err)
	}
	if !node.shared || node.bootstrap || resolved.Cluster.Address != "10.80.0.12" || resolved.DNS.Listen != "192.0.2.12,2001:db8::12" {
		t.Fatalf("wrong shared sample resolution: %+v", resolved)
	}
}
