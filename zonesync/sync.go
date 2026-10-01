package zonesync

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/abh/geodns/v3/zones"
)

const (
	Path       = "/api/v1/zones"
	StreamPath = "/api/v1/stream"
	NodesPath  = "/api/v1/nodes"
)

const maxSnapshotSize = 64 << 20

type snapshot struct {
	Version  int                        `json:"version"`
	Revision string                     `json:"revision"`
	Zones    map[string]json.RawMessage `json:"zones"`
}

type Master struct {
	dir       string
	token     string
	mu        sync.Mutex
	accepting bool
	latest    snapshot
	peers     map[string]*peer
	statuses  map[string]NodeStatus
}

func NewMaster(dir, token string) (*Master, error) {
	if token == "" {
		return nil, errors.New("sync token is required for master mode")
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("sync zone directory %q is unavailable", dir)
	}
	s, err := readSnapshot(dir)
	if err != nil {
		return nil, err
	}
	m := newMaster(token, s)
	m.dir = dir
	return m, nil
}

func newMaster(token string, s snapshot) *Master {
	return &Master{token: token, accepting: true, latest: s, peers: make(map[string]*peer), statuses: make(map[string]NodeStatus)}
}

func (m *Master) authorized(r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	return strings.HasPrefix(auth, "Bearer ") && subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(auth, "Bearer ")), []byte(m.token)) == 1
}

func (m *Master) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !m.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var s snapshot
	if m.dir != "" {
		var err error
		s, err = readSnapshot(m.dir)
		if err != nil {
			log.Printf("zone sync snapshot: %s", err)
			http.Error(w, "could not read zones", http.StatusServiceUnavailable)
			return
		}
	} else {
		m.mu.Lock()
		s = m.latest
		m.mu.Unlock()
	}
	data, err := json.Marshal(s)
	if err != nil || len(data) > maxSnapshotSize {
		http.Error(w, "zone snapshot is too large", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func readSnapshot(dir string) (snapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return snapshot{}, err
	}
	s := snapshot{Version: 1, Zones: make(map[string]json.RawMessage)}
	var size int64
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(strings.ToLower(name), ".json") || entry.IsDir() {
			continue
		}
		if !zoneFile(name) {
			return snapshot{}, fmt.Errorf("unsafe zone filename %q", name)
		}
		if !entry.Type().IsRegular() {
			return snapshot{}, fmt.Errorf("zone %q is not a regular file", name)
		}
		info, err := entry.Info()
		if err != nil {
			return snapshot{}, err
		}
		size += info.Size()
		if size > maxSnapshotSize {
			return snapshot{}, errors.New("zone snapshot exceeds size limit")
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return snapshot{}, err
		}
		if !json.Valid(data) {
			return snapshot{}, fmt.Errorf("zone %q contains invalid JSON", name)
		}
		s.Zones[name] = data
	}
	revision, err := revisionOfZones(s.Zones)
	if err != nil {
		return snapshot{}, err
	}
	s.Revision = revision
	return s, nil
}

func revisionOfZones(zones map[string]json.RawMessage) (string, error) {
	data, err := json.Marshal(zones)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

func zoneFile(name string) bool {
	if strings.HasPrefix(name, ".") || !strings.HasSuffix(strings.ToLower(name), ".json") {
		return false
	}
	if name == "" || filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
		return false
	}
	for _, c := range name {
		if c != '.' && c != '-' && c != '_' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

type Follower struct {
	dir      string
	url      string
	urls     []string
	token    string
	id       string
	nextURL  int
	reload   func() error
	client   *http.Client
	discover func(context.Context) (string, error)
}

func NewFollower(dir, masterURL, token, id string) (*Follower, error) {
	return NewFollowerWithURLs(dir, []string{masterURL}, token, id)
}

// NewFollowerWithURLs tries each controller when the current one is unavailable.
func NewFollowerWithURLs(dir string, masterURLs []string, token, id string) (*Follower, error) {
	f, err := newFollower(dir, token, id)
	if err != nil {
		return nil, err
	}
	if len(masterURLs) == 0 {
		return nil, errors.New("at least one sync URL is required")
	}
	urls := make([]string, 0, len(masterURLs))
	for _, masterURL := range masterURLs {
		if err := validateOrigin(masterURL); err != nil {
			return nil, err
		}
		urls = append(urls, strings.TrimSuffix(masterURL, "/")+Path)
	}
	f.url, f.urls = urls[0], urls
	return f, nil
}

func NewDiscoveredFollower(dir, token, id string, store *Store) (*Follower, error) {
	if store == nil {
		return nil, errors.New("etcd store is required for leader discovery")
	}
	f, err := newFollower(dir, token, id)
	if err != nil {
		return nil, err
	}
	f.discover = store.LeaderURL
	return f, nil
}

func validateOrigin(origin string) error {
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("sync URL must be an HTTP(S) origin: %q", origin)
	}
	return nil
}

func newFollower(dir, token, id string) (*Follower, error) {
	if token == "" {
		return nil, errors.New("sync token is required for follower mode")
	}
	if !validNodeID(id) {
		return nil, errors.New("sync follower id must contain 1-64 letters, digits, dots, underscores or hyphens")
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("sync zone directory %q is unavailable", dir)
	}
	return &Follower{dir: dir, token: token, id: id, client: &http.Client{Timeout: 15 * time.Second}}, nil
}

func (f *Follower) SetReload(fn func() error) { f.reload = fn }

func (f *Follower) Pull(ctx context.Context) error {
	target := f.url
	if f.discover != nil {
		origin, err := f.discover(ctx)
		if err != nil {
			return err
		}
		target = strings.TrimSuffix(origin, "/") + Path
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+f.token)
	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("master returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSnapshotSize+1))
	if err != nil {
		return err
	}
	if len(data) > maxSnapshotSize {
		return errors.New("zone snapshot exceeds size limit")
	}
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s.Version != 1 || s.Zones == nil {
		return errors.New("unsupported zone snapshot")
	}
	return f.apply(s)
}

func (f *Follower) apply(s snapshot) error {
	pending := make(map[string]string)
	defer func() {
		for _, name := range pending {
			os.Remove(name)
		}
	}()

	for name, data := range s.Zones {
		if !zoneFile(name) || !json.Valid(data) {
			return fmt.Errorf("invalid zone %q in snapshot", name)
		}
		path := filepath.Join(f.dir, name)
		if existing, err := os.ReadFile(path); err == nil && string(existing) == string(data) {
			continue
		}
		file, err := os.CreateTemp(f.dir, ".geodns-sync-*")
		if err != nil {
			return err
		}
		pending[name] = file.Name()
		if _, err := file.Write(data); err != nil {
			file.Close()
			return err
		}
		if err := file.Chmod(0644); err != nil {
			file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		zone := zones.NewZone(strings.TrimSuffix(name, filepath.Ext(name)))
		if err := zone.ReadZoneFile(file.Name()); err != nil {
			return fmt.Errorf("invalid zone %q: %w", name, err)
		}
	}
	for name, temp := range pending {
		if err := os.Rename(temp, filepath.Join(f.dir, name)); err != nil {
			return err
		}
		delete(pending, name)
	}
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !zoneFile(name) || entry.IsDir() {
			continue
		}
		if _, ok := s.Zones[name]; !ok {
			if err := os.Remove(filepath.Join(f.dir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}
