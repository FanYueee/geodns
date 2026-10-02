package zonesync

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"codeberg.org/miekg/dns/dnsutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const MasterPath = "/api/v1/master"
const maxZoneSize = 8 << 20

type leadership struct {
	ctx   context.Context
	key   string
	lease clientv3.LeaseID
	term  int64
}

// MasterInfo is discoverable from any live controller, including standbys.
type MasterInfo struct {
	ID     string `json:"id"`
	URL    string `json:"url"`
	Term   int64  `json:"term"`
	Weight int    `json:"weight"`
	key    string
	lease  clientv3.LeaseID
}

func (s *Store) Leader(ctx context.Context) (MasterInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	r, err := s.client.Get(ctx, s.prefix+"/election/", clientv3.WithFirstCreate()...)
	if err != nil {
		return MasterInfo{}, err
	}
	if len(r.Kvs) == 0 {
		return MasterInfo{}, errors.New("no elected master is available")
	}
	kv := r.Kvs[0]
	var address controllerAddress
	if err := json.Unmarshal(kv.Value, &address); err != nil || !validNodeID(address.ID) {
		return MasterInfo{}, errors.New("elected controller does not advertise a discovery address; use explicit sync urls for legacy controllers")
	}
	if err := validateOrigin(address.URL); err != nil {
		return MasterInfo{}, err
	}
	return MasterInfo{ID: address.ID, URL: address.URL, Weight: address.Weight, Term: kv.CreateRevision, key: string(kv.Key), lease: clientv3.LeaseID(kv.Lease)}, nil
}

// SetZoneMode must be called before Run. Empty retains legacy file publishing.
func (c *Cluster) SetZoneMode(mode string) error {
	var err error
	c.zoneMode, err = ZoneMode(mode)
	return err
}

func (c *Cluster) clearLeadership() {
	c.mu.Lock()
	c.leadership = nil
	c.mu.Unlock()
}

func apiJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(value)
}

func apiError(w http.ResponseWriter, code int, reason string, master *MasterInfo) {
	apiJSON(w, code, struct {
		Error  string      `json:"error"`
		Master *MasterInfo `json:"master,omitempty"`
	}{reason, master})
}

func (c *Cluster) authorizeAPI(w http.ResponseWriter, r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(auth, "Bearer ")), []byte(c.token)) != 1 {
		apiError(w, http.StatusUnauthorized, "unauthorized", nil)
		return false
	}
	return true
}

func (c *Cluster) ServeMaster(w http.ResponseWriter, r *http.Request) {
	if !c.authorizeAPI(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		apiError(w, http.StatusMethodNotAllowed, "method not allowed", nil)
		return
	}
	master, err := c.store.Leader(r.Context())
	if err != nil {
		w.Header().Set("Retry-After", "1")
		apiError(w, http.StatusServiceUnavailable, "master discovery unavailable", nil)
		return
	}
	apiJSON(w, http.StatusOK, master)
}

func (c *Cluster) requireLeader(w http.ResponseWriter, r *http.Request) *leadership {
	master, err := c.store.Leader(r.Context())
	if err != nil {
		apiError(w, http.StatusServiceUnavailable, "master discovery unavailable", nil)
		return nil
	}
	c.mu.RLock()
	l := c.leadership
	c.mu.RUnlock()
	if l == nil || l.ctx.Err() != nil || !c.IsLeader() || master.ID != c.id || master.key != l.key || master.Term != l.term || master.lease != l.lease || (r.Header.Get("X-Master-Term") != "" && r.Header.Get("X-Master-Term") != strconv.FormatInt(l.term, 10)) {
		apiError(w, http.StatusConflict, errLeadershipChanged.Error(), &master)
		return nil
	}
	w.Header().Set("X-Master-Term", strconv.FormatInt(l.term, 10))
	return l
}

func (c *Cluster) managementError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, errSnapshotTooLarge):
		apiError(w, http.StatusRequestEntityTooLarge, err.Error(), nil)
	case errors.Is(err, errPrecondition):
		apiError(w, http.StatusPreconditionFailed, err.Error(), nil)
	case errors.Is(err, errZoneNotFound):
		apiError(w, http.StatusNotFound, err.Error(), nil)
	case errors.Is(err, errLeadershipChanged):
		master, e := c.store.Leader(r.Context())
		if e != nil {
			apiError(w, http.StatusServiceUnavailable, "master discovery unavailable", nil)
		} else {
			apiError(w, http.StatusConflict, err.Error(), &master)
		}
	default:
		log.Printf("zone sync: master %q management request failed: %s", c.id, err)
		apiError(w, http.StatusServiceUnavailable, "operation unavailable; rediscover master and read current version before retrying", nil)
	}
}

// ServeZoneSet reads the committed set for listing/export in API mode, rather
// than the asynchronously activated copy used by legacy synchronization.
func (c *Cluster) ServeZoneSet(w http.ResponseWriter, r *http.Request) {
	if !c.authorizeAPI(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		apiError(w, http.StatusMethodNotAllowed, "method not allowed", nil)
		return
	}
	l := c.requireLeader(w, r)
	if l == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	snap, version, err := c.store.loadManaged(ctx, l)
	if err != nil {
		c.managementError(w, r, err)
		return
	}
	w.Header().Set("ETag", versionETag(version))
	apiJSON(w, http.StatusOK, snap)
}

func (c *Cluster) ServeZone(w http.ResponseWriter, r *http.Request) {
	if !c.authorizeAPI(w, r) {
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
		w.Header().Set("Allow", "GET, PUT, DELETE")
		apiError(w, http.StatusMethodNotAllowed, "method not allowed", nil)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, Path+"/")
	if name == "" || strings.Contains(name, "/") || !zoneFile(name+".json") || !dnsutil.IsName(name+".") {
		apiError(w, http.StatusBadRequest, "use a zone domain name without a path or .json suffix", nil)
		return
	}
	name += ".json"
	l := c.requireLeader(w, r)
	if l == nil {
		return
	}
	// Cancel in-flight work when the local leadership watch ends. The final etcd
	// transaction also fences the term and lease, including remote lease loss.
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	stop := context.AfterFunc(l.ctx, cancel)
	defer stop()
	if r.Method == http.MethodGet {
		snap, version, err := c.store.loadManaged(ctx, l)
		if err != nil {
			c.managementError(w, r, err)
			return
		}
		data, exists := snap.Zones[name]
		if !exists {
			c.managementError(w, r, errZoneNotFound)
			return
		}
		w.Header().Set("ETag", versionETag(version))
		w.Header().Set("X-Zone-Revision", snap.Revision)
		apiJSON(w, http.StatusOK, data)
		return
	}
	if c.zoneMode != "api" {
		apiError(w, http.StatusForbidden, "enable zone-mode = api on every controller to manage zones through the API", nil)
		return
	}
	match := r.Header.Get("If-Match")
	create := r.Method == http.MethodPut && r.Header.Get("If-None-Match") == "*"
	if (create && match != "") || (!create && (match == "" || r.Header.Get("If-None-Match") != "")) {
		apiError(w, http.StatusPreconditionRequired, "update/delete requires If-Match with the GET ETag; create requires If-None-Match: *", nil)
		return
	}
	var data []byte
	if r.Method == http.MethodPut {
		var err error
		data, err = io.ReadAll(http.MaxBytesReader(w, r.Body, maxZoneSize))
		if err != nil {
			var limit *http.MaxBytesError
			code := http.StatusBadRequest
			if errors.As(err, &limit) {
				code = http.StatusRequestEntityTooLarge
			}
			apiError(w, code, "could not read zone JSON (maximum 8 MiB)", nil)
			return
		}
		if err := validateZoneData(name, data); err != nil {
			apiError(w, http.StatusBadRequest, "invalid zone: "+err.Error(), nil)
			return
		}
	}
	snap, version, err := c.store.mutateZone(ctx, l, name, data, match, create)
	if err != nil {
		c.managementError(w, r, err)
		return
	}
	w.Header().Set("ETag", versionETag(version))
	w.Header().Set("X-Zone-Revision", snap.Revision)
	code := http.StatusOK
	if create {
		code = http.StatusCreated
	}
	log.Printf("zone sync: master %q committed %s zone %q (term %d, version %d, revision %s)", c.id, r.Method, name, l.term, version, snap.Revision)
	apiJSON(w, code, struct {
		Revision  string `json:"revision"`
		Version   int64  `json:"version"`
		Committed bool   `json:"committed"`
	}{snap.Revision, version, true})
}

// RegisterManagementRoutes adds HA management endpoints without changing the
// legacy single-master sync handler interface.
func RegisterManagementRoutes(mux *http.ServeMux, handler http.Handler) {
	if c, ok := handler.(*Cluster); ok {
		mux.HandleFunc(MasterPath, c.ServeMaster)
		mux.HandleFunc(Path+"/", c.ServeZone)
	}
}
