package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/abh/geodns/v3/appconfig"
)

func TestConfigLocationAcceptsFileAndDirectory(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "geodns.conf")
	if err := os.WriteFile(file, []byte("[dns]\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, location := range []string{dir, file} {
		gotDir, gotFile, err := configLocation(location, "geodns.conf", false)
		if err != nil || gotDir != dir || gotFile != file {
			t.Fatalf("config location %q: %q %q %v", location, gotDir, gotFile, err)
		}
	}
	if _, _, err := configLocation(file, "other.conf", true); err == nil {
		t.Fatal("conflicting config filename accepted")
	}
	if _, got, err := configLocation(dir, "custom.conf", true); err != nil || got != filepath.Join(dir, "custom.conf") {
		t.Fatalf("legacy filename selection: %q %v", got, err)
	}
}

func TestDNSListenConfigHonorsFlags(t *testing.T) {
	cfg := appconfig.AppConfig{}
	cfg.DNS.Listen, cfg.DNS.Port = "192.0.2.1,2001:db8::1", "5053"
	address, port := dnsListenConfig(cfg, "*", "53", false, false)
	if address != cfg.DNS.Listen || port != cfg.DNS.Port {
		t.Fatalf("config listen settings ignored: %s %s", address, port)
	}
	address, port = dnsListenConfig(cfg, "127.0.0.1", "5353", true, true)
	if address != "127.0.0.1" || port != "5353" {
		t.Fatalf("config overrode command line flags: %s %s", address, port)
	}
}
