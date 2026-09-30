package zonesync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testZone = `{"data":{"www":{"a":[["192.0.2.1",1]]}}}`

func writeZone(t *testing.T, dir, name, data string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestMasterFollowerSync(t *testing.T) {
	masterDir := t.TempDir()
	followerDir := t.TempDir()
	writeZone(t, masterDir, "example.com.json", testZone)
	writeZone(t, followerDir, "geodns.conf", "[sync]\nmode = follower\n")

	master, err := NewMaster(masterDir, "secret")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(master)
	defer server.Close()

	res, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("without token: got HTTP %d", res.StatusCode)
	}

	follower, err := NewFollower(followerDir, server.URL, "secret", "1s")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := follower.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	checkZoneIP(t, followerDir, "example.com.json", "192.0.2.1")

	writeZone(t, masterDir, "example.com.json", `{"data":{"www":{"a":[["192.0.2.2",1]]}}}`)
	writeZone(t, masterDir, "new.example.json", testZone)
	if err := follower.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	checkZoneIP(t, followerDir, "example.com.json", "192.0.2.2")
	checkZoneIP(t, followerDir, "new.example.json", "192.0.2.1")

	if err := os.Remove(filepath.Join(masterDir, "example.com.json")); err != nil {
		t.Fatal(err)
	}
	if err := follower.Pull(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(followerDir, "example.com.json")); !os.IsNotExist(err) {
		t.Fatalf("deleted zone still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(followerDir, "geodns.conf")); err != nil {
		t.Fatalf("local config was removed: %v", err)
	}

	writeZone(t, masterDir, "new.example.json", `{broken`)
	if err := follower.Pull(ctx); err == nil {
		t.Fatal("invalid master JSON was accepted")
	}
	checkZoneIP(t, followerDir, "new.example.json", "192.0.2.1")
}

func TestFollowerRejectsInvalidSnapshotBeforeWriting(t *testing.T) {
	dir := t.TempDir()
	writeZone(t, dir, "existing.example.json", testZone)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(snapshot{
			Version: 1,
			Zones: map[string]json.RawMessage{
				"existing.example.json": json.RawMessage(`{"data":{"www":{"a":[["192.0.2.2",1]]}}}`),
				"bad.example.json":      json.RawMessage(`{"data":{"www":{"a":[["bad-ip",1]]}}}`),
			},
		})
	}))
	defer server.Close()
	follower, err := NewFollower(dir, server.URL, "secret", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.Pull(context.Background()); err == nil {
		t.Fatal("invalid zone was accepted")
	}
	checkZoneIP(t, dir, "existing.example.json", "192.0.2.1")
	if _, err := os.Stat(filepath.Join(dir, "bad.example.json")); !os.IsNotExist(err) {
		t.Fatalf("invalid zone was written: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".geodns-sync-") {
			t.Fatalf("temporary file was not removed: %s", entry.Name())
		}
	}
}

func TestFollowerRejectsUnsafeNames(t *testing.T) {
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(snapshot{Version: 1, Zones: map[string]json.RawMessage{"../outside.json": json.RawMessage(testZone)}})
	}))
	defer server.Close()
	follower, err := NewFollower(dir, server.URL, "secret", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.Pull(context.Background()); err == nil {
		t.Fatal("unsafe zone name was accepted")
	}
}

func checkZoneIP(t *testing.T, dir, name, ip string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	var zone struct {
		Data map[string]struct {
			A [][]any `json:"a"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &zone); err != nil {
		t.Fatal(err)
	}
	if got := zone.Data["www"].A[0][0]; got != ip {
		t.Fatalf("%s: got IP %v, want %s", name, got, ip)
	}
}
