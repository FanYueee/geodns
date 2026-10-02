package zonesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/abh/geodns/v3/zones"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	snapshotChunkSize = 512 << 10
	maxStoredSnapshot = maxSnapshotSize + 1<<20
)

var ErrNoSnapshot = errors.New("no zone snapshot has been published")

type storedSnapshot struct {
	Revision string `json:"revision"`
	Chunks   int    `json:"chunks"`
}

type Store struct {
	client    *clientv3.Client
	prefix    string
	closeOnce sync.Once
	closeErr  error
}

func NewStore(client *clientv3.Client, prefix string) (*Store, error) {
	if client == nil {
		return nil, errors.New("etcd client is required")
	}
	if prefix == "/" || !strings.HasPrefix(prefix, "/") || path.Clean(prefix) != prefix {
		return nil, fmt.Errorf("invalid etcd prefix %q", prefix)
	}
	return &Store{client: client, prefix: prefix}, nil
}

func (s *Store) currentKey() string { return s.prefix + "/current" }

func (s *Store) snapshotPrefix(revision string) string {
	return s.prefix + "/snapshots/" + revision + "/"
}

func (s *Store) chunkKey(revision string, index int) string {
	return fmt.Sprintf("%s%04d", s.snapshotPrefix(revision), index)
}

func (s *Store) nodePrefix() string { return s.prefix + "/nodes/" }

func (s *Store) loadNodeStatuses(ctx context.Context) (map[string]NodeStatus, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := s.client.Get(requestCtx, s.nodePrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	statuses := make(map[string]NodeStatus, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		id := strings.TrimPrefix(string(kv.Key), s.nodePrefix())
		var status NodeStatus
		if !validNodeID(id) || json.Unmarshal(kv.Value, &status) != nil || status.ID != id {
			log.Printf("zone sync: ignoring invalid stored status for node %q", id)
			continue
		}
		status.Connected = false
		status.LastError = "awaiting reconnect to new controller"
		statuses[id] = status
	}
	return statuses, nil
}

func (s *Store) saveNodeStatuses(ctx context.Context, leaderKey string, lease clientv3.LeaseID, statuses []NodeStatus) error {
	for start := 0; start < len(statuses); start += 64 {
		end := min(start+64, len(statuses))
		ops := make([]clientv3.Op, 0, end-start)
		for _, status := range statuses[start:end] {
			data, err := json.Marshal(status)
			if err != nil {
				return err
			}
			ops = append(ops, clientv3.OpPut(s.nodePrefix()+status.ID, string(data)))
		}
		requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		result, err := s.client.Txn(requestCtx).
			If(clientv3.Compare(clientv3.LeaseValue(leaderKey), "=", int64(lease))).
			Then(ops...).Commit()
		cancel()
		if err != nil {
			return err
		}
		if !result.Succeeded {
			return errors.New("controller lost its election lease")
		}
	}
	return nil
}

func (s *Store) currentMetadata(ctx context.Context) (storedSnapshot, int64, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := s.client.Get(requestCtx, s.currentKey())
	if err != nil {
		return storedSnapshot{}, 0, err
	}
	if len(resp.Kvs) == 0 {
		return storedSnapshot{}, resp.Header.Revision, ErrNoSnapshot
	}
	var meta storedSnapshot
	if err := json.Unmarshal(resp.Kvs[0].Value, &meta); err != nil {
		return storedSnapshot{}, 0, fmt.Errorf("invalid published snapshot metadata: %w", err)
	}
	if len(meta.Revision) != 64 || meta.Chunks < 1 || meta.Chunks > (maxStoredSnapshot+snapshotChunkSize-1)/snapshotChunkSize {
		return storedSnapshot{}, 0, errors.New("invalid published snapshot metadata")
	}
	return meta, resp.Header.Revision, nil
}

func (s *Store) loadCurrent(ctx context.Context) (snapshot, int64, error) {
	meta, etcdRevision, err := s.currentMetadata(ctx)
	if err != nil {
		return snapshot{}, etcdRevision, err
	}
	return s.loadSnapshot(ctx, meta, etcdRevision)
}

func (s *Store) loadSnapshot(ctx context.Context, meta storedSnapshot, etcdRevision int64) (snapshot, int64, error) {
	data := make([]byte, 0, meta.Chunks*snapshotChunkSize)
	for i := 0; i < meta.Chunks; i++ {
		requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		resp, err := s.client.Get(requestCtx, s.chunkKey(meta.Revision, i), clientv3.WithRev(etcdRevision))
		cancel()
		if err != nil {
			return snapshot{}, 0, err
		}
		if len(resp.Kvs) != 1 || len(data)+len(resp.Kvs[0].Value) > maxStoredSnapshot {
			return snapshot{}, 0, fmt.Errorf("published snapshot %s is missing or too large", meta.Revision)
		}
		data = append(data, resp.Kvs[0].Value...)
	}
	var result snapshot
	if err := json.Unmarshal(data, &result); err != nil {
		return snapshot{}, 0, fmt.Errorf("invalid published snapshot: %w", err)
	}
	if result.Version != 1 || result.Zones == nil || result.Revision != meta.Revision {
		return snapshot{}, 0, errors.New("published snapshot version or revision is invalid")
	}
	for name, zone := range result.Zones {
		if !zoneFile(name) || !json.Valid(zone) {
			return snapshot{}, 0, fmt.Errorf("invalid published zone %q", name)
		}
	}
	revision, err := revisionOfZones(result.Zones)
	if err != nil || revision != meta.Revision {
		return snapshot{}, 0, errors.New("published snapshot checksum does not match")
	}
	return result, etcdRevision, nil
}

// Publish stores a complete snapshot and atomically switches the active revision.
func (s *Store) Publish(ctx context.Context, dir string) (string, error) {
	return s.publish(ctx, dir, false, false)
}

// Bootstrap imports local zones only when the cluster has no published snapshot.
func (s *Store) Bootstrap(ctx context.Context, dir string) (string, error) {
	meta, _, err := s.currentMetadata(ctx)
	if err == nil {
		return meta.Revision, nil
	}
	if !errors.Is(err, ErrNoSnapshot) {
		return "", err
	}
	return s.publish(ctx, dir, true, true)
}

func (s *Store) publish(ctx context.Context, dir string, bootstrap, requireZones bool) (string, error) {
	mutex, release, err := s.lockPublication(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	if !bootstrap {
		if err := s.checkZoneMode(ctx, "files"); err != nil {
			return "", err
		}
	}
	previous, _, err := s.currentMetadata(ctx)
	if err != nil && !errors.Is(err, ErrNoSnapshot) {
		return "", err
	}
	if bootstrap && err == nil {
		return previous.Revision, nil
	}
	snapshot, err := readSnapshot(dir)
	if err != nil {
		return "", err
	}
	if requireZones && len(snapshot.Zones) == 0 {
		return "", errors.New("source zone directory has no zone JSON files")
	}
	for name := range snapshot.Zones {
		zone := zones.NewZone(strings.TrimSuffix(name, filepath.Ext(name)))
		if err := zone.ReadZoneFile(filepath.Join(dir, name)); err != nil {
			return "", fmt.Errorf("invalid zone %q: %w", name, err)
		}
	}
	verified, err := readSnapshot(dir)
	if err != nil || verified.Revision != snapshot.Revision {
		return "", errors.New("zone files changed while publishing; retry")
	}
	if previous.Revision == snapshot.Revision {
		return snapshot.Revision, nil
	}
	guards := []clientv3.Cmp{}
	if bootstrap {
		guards = append(guards, clientv3.Compare(clientv3.Version(s.currentKey()), "=", 0))
	}
	_, err = s.commitSnapshot(ctx, mutex, snapshot, previous.Revision, guards)
	return snapshot.Revision, err
}

func (s *Store) lockPublication(ctx context.Context) (*concurrency.Mutex, func(), error) {
	session, err := concurrency.NewSession(s.client, concurrency.WithTTL(30), concurrency.WithContext(ctx))
	if err != nil {
		return nil, nil, err
	}
	mutex := concurrency.NewMutex(session, s.prefix+"/publish-lock")
	if err := mutex.Lock(ctx); err != nil {
		session.Close()
		return nil, nil, err
	}
	return mutex, func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mutex.Unlock(unlockCtx); err != nil {
			log.Printf("zone sync: release publish lock: %s", err)
		}
		session.Close()
	}, nil
}

// All publishers serialize chunk creation and pruning with the same leased lock.
// Management writes additionally fence the election term and current version.
func (s *Store) commitSnapshot(ctx context.Context, mutex *concurrency.Mutex, snapshot snapshot, previous string, guards []clientv3.Cmp) (int64, error) {
	data, err := json.Marshal(snapshot)
	if err != nil || len(data) > maxSnapshotSize {
		return 0, errSnapshotTooLarge
	}
	chunks := (len(data) + snapshotChunkSize - 1) / snapshotChunkSize
	for i := 0; i < chunks; i++ {
		end := min((i+1)*snapshotChunkSize, len(data))
		requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := s.client.Put(requestCtx, s.chunkKey(snapshot.Revision, i), string(data[i*snapshotChunkSize:end]))
		cancel()
		if err != nil {
			return 0, err
		}
	}
	meta, err := json.Marshal(storedSnapshot{Revision: snapshot.Revision, Chunks: chunks})
	if err != nil {
		return 0, err
	}
	guards = append(guards, mutex.IsOwner())
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	commit, err := s.client.Txn(requestCtx).If(guards...).Then(clientv3.OpPut(s.currentKey(), string(meta))).Commit()
	cancel()
	if err != nil {
		return 0, err
	}
	if !commit.Succeeded {
		return 0, errLeadershipChanged
	}
	if err := s.prune(ctx, mutex, snapshot.Revision, previous); err != nil {
		log.Printf("zone sync: prune old snapshots: %s", err)
	}
	return commit.Header.Revision, nil
}

func (s *Store) prune(ctx context.Context, mutex *concurrency.Mutex, current, previous string) error {
	root := s.prefix + "/snapshots/"
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	resp, err := s.client.Get(requestCtx, root, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	cancel()
	if err != nil {
		return err
	}
	old := make(map[string]bool)
	for _, kv := range resp.Kvs {
		revision, _, ok := strings.Cut(strings.TrimPrefix(string(kv.Key), root), "/")
		if ok && revision != current && revision != previous {
			old[revision] = true
		}
	}
	for revision := range old {
		requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		result, err := s.client.Txn(requestCtx).If(mutex.IsOwner()).Then(clientv3.OpDelete(s.snapshotPrefix(revision), clientv3.WithPrefix())).Commit()
		cancel()
		if err != nil {
			return err
		}
		if !result.Succeeded {
			return errors.New("publish lease expired during snapshot cleanup")
		}
	}
	return nil
}
