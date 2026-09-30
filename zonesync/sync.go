package zonesync

import (
	"context"
	"crypto/subtle"
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
	"time"

	"github.com/abh/geodns/v3/zones"
)

const Path = "/api/v1/zones"

const maxSnapshotSize = 64 << 20

type snapshot struct {
	Version int                        `json:"version"`
	Zones   map[string]json.RawMessage `json:"zones"`
}

type Master struct {
	dir   string
	token string
}

func NewMaster(dir, token string) (*Master, error) {
	if token == "" {
		return nil, errors.New("sync token is required for master mode")
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("sync zone directory %q is unavailable", dir)
	}
	return &Master{dir: dir, token: token}, nil
}

func (m *Master) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(auth, "Bearer ")), []byte(m.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	s, err := readSnapshot(m.dir)
	if err != nil {
		log.Printf("zone sync snapshot: %s", err)
		http.Error(w, "could not read zones", http.StatusServiceUnavailable)
		return
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
	return s, nil
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
	token    string
	interval time.Duration
	client   *http.Client
}

func NewFollower(dir, masterURL, token, interval string) (*Follower, error) {
	if token == "" {
		return nil, errors.New("sync token is required for follower mode")
	}
	u, err := url.Parse(masterURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("sync URL must be an HTTP(S) origin: %q", masterURL)
	}
	if interval == "" {
		interval = "30s"
	}
	period, err := time.ParseDuration(interval)
	if err != nil || period < time.Second {
		return nil, fmt.Errorf("sync interval must be at least 1s: %q", interval)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("sync zone directory %q is unavailable", dir)
	}
	return &Follower{dir: dir, url: strings.TrimSuffix(masterURL, "/") + Path, token: token, interval: period, client: &http.Client{Timeout: 15 * time.Second}}, nil
}

func (f *Follower) Run(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		if err := f.Pull(ctx); err != nil && ctx.Err() == nil {
			log.Printf("zone sync: %s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (f *Follower) Pull(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, nil)
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
