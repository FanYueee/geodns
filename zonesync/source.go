package zonesync

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// WatchSource bootstraps an empty cluster and publishes subsequent local edits.
// The startup revision is a baseline, so unchanged stale files never replace
// an existing cluster snapshot after a restart.
func (s *Store) WatchSource(ctx context.Context, dir string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()
	addWatches := func() {
		// Watching the parent also catches replacement of the source directory.
		for _, path := range []string{filepath.Dir(dir), dir} {
			if err := watcher.Add(path); err != nil {
				log.Printf("zone sync: source watcher %q: %s", path, err)
			}
		}
	}
	addWatches()
	initial, _ := readSnapshot(dir)
	localRevision := initial.Revision
	ready := false
	attempt := func() {
		attemptCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if !ready {
			revision, err := s.Bootstrap(attemptCtx, dir)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("zone sync: bootstrap failed: %s", err)
				}
				return
			}
			ready = true
			log.Printf("zone sync: bootstrap ready; watching %q; cluster revision %s", dir, revision)
			return
		}
		candidate, err := readSnapshot(dir)
		if err == nil && candidate.Revision == localRevision {
			return
		}
		if err == nil && len(candidate.Zones) == 0 {
			err = errors.New("source directory has no zone JSON files; keeping published zones (use -publish to clear all zones explicitly)")
		}
		if err == nil {
			var revision string
			revision, err = s.publish(attemptCtx, dir, false, true)
			if err == nil {
				localRevision = revision
				log.Printf("zone sync: source %q automatically published revision %s", dir, revision)
				return
			}
		}
		if ctx.Err() == nil {
			log.Printf("zone sync: source auto-publish failed; keeping published zones: %s", err)
		}
	}
	attempt()

	// Scan local files to recover missed events and retry failed publishes.
	// Unchanged files never trigger etcd writes or node snapshot polling.
	retry := time.NewTicker(5 * time.Second)
	defer retry.Stop()
	settle := time.NewTimer(time.Hour)
	settle.Stop()
	defer settle.Stop()
	var pending <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return errors.New("source directory watcher stopped")
			}
			name := filepath.Clean(event.Name)
			if name != dir && (filepath.Dir(name) != dir || !zoneFile(filepath.Base(name))) {
				continue
			}
			if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
			if !settle.Stop() {
				select {
				case <-settle.C:
				default:
				}
			}
			settle.Reset(500 * time.Millisecond)
			pending = settle.C
		case <-pending:
			pending = nil
			addWatches()
			attempt()
		case <-retry.C:
			if pending == nil {
				addWatches()
				attempt()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return errors.New("source directory watcher stopped")
			}
			log.Printf("zone sync: source directory watcher: %s", err)
		}
	}
}
