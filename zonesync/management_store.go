package zonesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/abh/geodns/v3/zones"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	errLeadershipChanged = errors.New("master changed; discover the master again")
	errAPIMode           = errors.New("cluster uses zone-mode = api; local file publication is disabled")
	errPrecondition      = errors.New("zone version changed; read the latest version before retrying")
	errZoneNotFound      = errors.New("zone not found")
	errSnapshotTooLarge  = errors.New("complete zone snapshot exceeds 64 MiB sync limit")
)

// ZoneMode keeps existing deployments in file mode unless API mode is selected.
func ZoneMode(mode string) (string, error) {
	if mode == "" {
		return "files", nil
	}
	if mode != "api" && mode != "files" {
		return "", errors.New("zone-mode must be api or files")
	}
	return mode, nil
}

func (s *Store) checkZoneMode(ctx context.Context, mode string) error {
	r, err := s.client.Get(ctx, s.prefix+"/zone-mode")
	if err != nil {
		return err
	}
	api := len(r.Kvs) != 0 && string(r.Kvs[0].Value) == "api"
	if api != (mode == "api") {
		return errAPIMode
	}
	return nil
}

// Persist API authority under the publication lock. Publication commands using
// stale file-mode configs cannot replace API data with an entire local directory.
func (s *Store) configureZoneMode(ctx context.Context, mode string) error {
	mode, err := ZoneMode(mode)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if mode == "files" {
		return s.checkZoneMode(ctx, mode)
	}
	mutex, release, err := s.lockPublication(ctx)
	if err != nil {
		return err
	}
	defer release()
	r, err := s.client.Txn(ctx).If(mutex.IsOwner()).Then(clientv3.OpPut(s.prefix+"/zone-mode", "api")).Commit()
	if err != nil {
		return err
	}
	if !r.Succeeded {
		return errLeadershipChanged
	}
	return nil
}

// ImportSource retries initial import, then stops permanently. Local edits and
// restarts never replace data that has already been committed to the cluster.
func (s *Store) ImportSource(ctx context.Context, dir string) error {
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 15*time.Second)
		revision, err := s.Bootstrap(attempt, dir)
		cancel()
		if err == nil {
			log.Printf("zone sync: initial import ready; cluster revision %s; manage further updates through master API", revision)
			return nil
		}
		if ctx.Err() == nil {
			log.Printf("zone sync: initial import failed: %s; retrying", err)
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

func validateZoneData(name string, data []byte) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil || obj == nil {
		return errors.New("zone must be a JSON object")
	}
	f, err := os.CreateTemp("", "geodns-zone-validation-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return zones.NewZone(strings.TrimSuffix(name, ".json")).ReadZoneFile(f.Name())
}

func (l *leadership) guards() []clientv3.Cmp {
	return []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(l.key), "=", l.term),
		clientv3.Compare(clientv3.LeaseValue(l.key), "=", int64(l.lease)),
	}
}

// loadManaged linearizes the snapshot selection with election ownership. Chunks
// are then read at that revision, so pruning cannot race a reader.
func (s *Store) loadManaged(ctx context.Context, l *leadership) (snapshot, int64, error) {
	r, err := s.client.Txn(ctx).If(l.guards()...).Then(clientv3.OpGet(s.currentKey())).Commit()
	if err != nil {
		return snapshot{}, 0, err
	}
	if !r.Succeeded {
		return snapshot{}, 0, errLeadershipChanged
	}
	kvs := r.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 {
		snap := snapshot{Version: 1, Zones: make(map[string]json.RawMessage)}
		snap.Revision, _ = revisionOfZones(snap.Zones)
		return snap, 0, nil
	}
	var meta storedSnapshot
	if err := json.Unmarshal(kvs[0].Value, &meta); err != nil {
		return snapshot{}, 0, err
	}
	if len(meta.Revision) != 64 || meta.Chunks < 1 || meta.Chunks > (maxStoredSnapshot+snapshotChunkSize-1)/snapshotChunkSize {
		return snapshot{}, 0, errors.New("invalid snapshot metadata")
	}
	snap, _, err := s.loadSnapshot(ctx, meta, r.Header.Revision)
	return snap, kvs[0].ModRevision, err
}

// Versions are etcd modification revisions of the whole Zone set. They remain
// monotonic even if content changes A -> B -> A, preventing stale overwrites.
func versionETag(version int64) string { return fmt.Sprintf("\"%d\"", version) }

func (s *Store) mutateZone(ctx context.Context, l *leadership, name string, data []byte, match string, create bool) (snapshot, int64, error) {
	mutex, release, err := s.lockPublication(ctx)
	if err != nil {
		return snapshot{}, 0, err
	}
	defer release()
	if err := s.checkZoneMode(ctx, "api"); err != nil {
		return snapshot{}, 0, err
	}
	current, version, err := s.loadManaged(ctx, l)
	if err != nil {
		return snapshot{}, 0, err
	}
	_, exists := current.Zones[name]
	if create {
		if exists || data == nil {
			return snapshot{}, 0, errPrecondition
		}
	} else if match != versionETag(version) || !exists {
		return snapshot{}, 0, errPrecondition
	}
	previous := current.Revision
	if data == nil {
		delete(current.Zones, name)
	} else {
		current.Zones[name] = json.RawMessage(data)
	}
	current.Revision, err = revisionOfZones(current.Zones)
	if err != nil {
		return snapshot{}, 0, err
	}
	guards := append(l.guards(), clientv3.Compare(clientv3.Value(s.prefix+"/zone-mode"), "=", "api"), clientv3.Compare(clientv3.ModRevision(s.currentKey()), "=", version))
	newVersion, err := s.commitSnapshot(ctx, mutex, current, previous, guards)
	return current, newVersion, err
}
