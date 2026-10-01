package zonesync

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func startSourceWatcher(t *testing.T, store *Store, dir string) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- store.WatchSource(ctx, dir) }()
	stop := func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("source watcher stopped: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("source watcher did not stop")
		}
	}
	return stop
}

func TestSourceWatcherPublishesEditsAndKeepsLastValidSnapshot(t *testing.T) {
	store, err := NewStore(testEtcd(t), "/test/source-edits")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeZone(t, dir, "example.com.json", testZone)
	stop := startSourceWatcher(t, store, dir)
	defer stop()
	waitZone := func(name, value string, count int) {
		t.Helper()
		eventually(t, func() bool {
			snapshot, _, err := store.loadCurrent(context.Background())
			return err == nil && len(snapshot.Zones) == count && string(snapshot.Zones[name]) == value
		})
	}
	waitZone("example.com.json", testZone, 1)
	updated := strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.2")
	// A temporary file renamed over the original is a common editor save.
	writeZone(t, dir, ".example.tmp", updated)
	if err := os.Rename(filepath.Join(dir, ".example.tmp"), filepath.Join(dir, "example.com.json")); err != nil {
		t.Fatal(err)
	}
	waitZone("example.com.json", updated, 1)
	valid, _, err := store.currentMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{`{"data":`, `{"data":{},"targeting":"invalid-target"}`} {
		writeZone(t, dir, "example.com.json", invalid)
		time.Sleep(time.Second)
		current, _, err := store.currentMetadata(context.Background())
		if err != nil || current.Revision != valid.Revision {
			t.Fatalf("invalid edit replaced the snapshot: %+v, %v", current, err)
		}
	}
	writeZone(t, dir, "example.com.json", updated)
	writeZone(t, dir, "other.example.json", testZone)
	waitZone("other.example.json", testZone, 2)
	if err := os.Remove(filepath.Join(dir, "other.example.json")); err != nil {
		t.Fatal(err)
	}
	waitZone("example.com.json", updated, 1)
	if err := os.Remove(filepath.Join(dir, "example.com.json")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	waitZone("example.com.json", updated, 1)
	// Replacing the watched directory must not disable future hot updates.
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	writeZone(t, dir, "example.com.json", testZone)
	waitZone("example.com.json", testZone, 1)
}

func TestSourceWatcherRestartDoesNotOverwriteCluster(t *testing.T) {
	store, err := NewStore(testEtcd(t), "/test/source-restart")
	if err != nil {
		t.Fatal(err)
	}
	stale, fresh := t.TempDir(), t.TempDir()
	writeZone(t, stale, "example.com.json", testZone)
	updated := strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.3")
	writeZone(t, fresh, "example.com.json", updated)
	revision, err := store.Publish(context.Background(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	stop := startSourceWatcher(t, store, stale)
	defer stop()
	// Include the recovery scan, not just the initial bootstrap path.
	time.Sleep(6 * time.Second)
	current, _, err := store.currentMetadata(context.Background())
	if err != nil || current.Revision != revision {
		t.Fatalf("stale startup files replaced cluster data: %+v, %v", current, err)
	}
	// An explicit edit of that source is still accepted after restart.
	edited := strings.ReplaceAll(testZone, "192.0.2.1", "192.0.2.4")
	writeZone(t, stale, "example.com.json", edited)
	eventually(t, func() bool {
		snapshot, _, err := store.loadCurrent(context.Background())
		return err == nil && string(snapshot.Zones["example.com.json"]) == edited
	})
}
