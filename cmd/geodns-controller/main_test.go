package main

import (
	"path/filepath"
	"testing"

	"github.com/abh/geodns/v3/appconfig"
	"go.etcd.io/etcd/server/v3/embed"
	"gopkg.in/gcfg.v1"
)

func TestEtcdConfigSample(t *testing.T) {
	name := filepath.Join("..", "..", "dns", "etcd.ha.yml.sample")
	cfg, err := embed.ConfigFromFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Name != "controller-1" || len(cfg.ListenClientUrls) != 1 || cfg.ListenClientUrls[0].Host != "10.80.0.11:2379" {
		t.Fatalf("unexpected etcd settings: name=%q, client urls=%v", cfg.Name, cfg.ListenClientUrls)
	}
}

func TestHAConfigSample(t *testing.T) {
	var cfg appconfig.AppConfig
	name := filepath.Join("..", "..", "dns", "geodns.controller.ha.conf.sample")
	if err := gcfg.ReadFileInto(&cfg, name); err != nil {
		t.Fatal(err)
	}
	if cfg.Sync.Mode != "controller" || cfg.Sync.Token == "" {
		t.Fatalf("invalid sync settings: %+v", cfg.Sync)
	}
	o := controllerOptions{mode: "single", listen: ":8053", zoneDir: "./dns/", etcdPrefix: "/geodns"}
	o.applyConfig(cfg.Controller, name, nil)
	if o.mode != "ha" || o.id != "controller-1" || o.listen != "10.80.0.11:8053" || o.zoneDir != "/srv/geodns/zones" || o.etcdPrefix != "/geodns/production" {
		t.Fatalf("unexpected controller settings: %+v", o)
	}
	if len(o.etcdEndpoints) == 0 {
		t.Fatal("sample is missing etcd endpoints")
	}
}

func TestControllerFlagsOverrideConfig(t *testing.T) {
	cfg := appconfig.ControllerConfig{
		Mode: "ha", ID: "from-file", Listen: "127.0.0.1:8053",
		ZoneDirectory: "zones", EtcdEndpoints: "http://127.0.0.1:2379",
		EtcdPasswordFile: "password",
	}
	o := controllerOptions{mode: "single", id: "from-flag", zoneDir: "/other/zones", etcdEndpoints: "http://other:2379"}
	o.applyConfig(cfg, "/etc/geodns/controller.conf", map[string]bool{"mode": true, "id": true, "config": true, "etcd": true})
	if o.mode != "single" || o.id != "from-flag" || o.zoneDir != "/other/zones" || o.etcdEndpoints != "http://other:2379" {
		t.Fatalf("explicit flags were overridden: %+v", o)
	}
	if o.listen != "127.0.0.1:8053" || o.etcdPasswordFile != "/etc/geodns/password" {
		t.Fatalf("unset flags did not use config values: %+v", o)
	}
}
