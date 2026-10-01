package zonesync

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Cluster serves sync requests only while this controller holds the etcd lease.
type Cluster struct {
	store     *Store
	id        string
	token     string
	leaseTTL  int
	mu        sync.RWMutex
	master    *Master
	advertise string
}

type controllerAddress struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// SetAdvertiseURL configures discovery before Run starts.
func (c *Cluster) SetAdvertiseURL(origin string) error {
	if err := validateOrigin(origin); err != nil {
		return err
	}
	u, _ := url.Parse(origin)
	if ip := net.ParseIP(u.Hostname()); ip != nil && ip.IsUnspecified() {
		return errors.New("advertise address must be reachable; a wildcard listen address cannot be advertised")
	}
	c.advertise = origin
	return nil
}

// LeaderURL reads the first election candidate, whose key shares its session lease.
func (s *Store) LeaderURL(ctx context.Context) (string, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := s.client.Get(requestCtx, s.prefix+"/election/", clientv3.WithFirstCreate()...)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", errors.New("no controller has joined the election")
	}
	var address controllerAddress
	if err := json.Unmarshal(resp.Kvs[0].Value, &address); err != nil || !validNodeID(address.ID) {
		return "", errors.New("elected controller does not advertise a discovery address; use explicit sync urls for legacy controllers")
	}
	if err := validateOrigin(address.URL); err != nil {
		return "", err
	}
	return address.URL, nil
}

func NewCluster(store *Store, id, token string) (*Cluster, error) {
	if store == nil {
		return nil, errors.New("etcd snapshot store is required")
	}
	if !validNodeID(id) {
		return nil, errors.New("controller id must contain 1-64 letters, digits, dots, underscores or hyphens")
	}
	if token == "" {
		return nil, errors.New("sync token is required")
	}
	return &Cluster{store: store, id: id, token: token, leaseTTL: 10}, nil
}

func (c *Cluster) Run(ctx context.Context) error {
	value := c.id
	if c.advertise != "" {
		data, err := json.Marshal(controllerAddress{ID: c.id, URL: c.advertise})
		if err != nil {
			return err
		}
		value = string(data)
	}
	defer c.demote()
	for ctx.Err() == nil {
		session, err := concurrency.NewSession(c.store.client, concurrency.WithTTL(c.leaseTTL), concurrency.WithContext(ctx))
		if err != nil {
			log.Printf("zone sync: controller %q cannot create election lease: %s", c.id, err)
			if !retryCluster(ctx) {
				break
			}
			continue
		}
		election := concurrency.NewElection(session, c.store.prefix+"/election")
		if err := election.Campaign(ctx, value); err != nil {
			session.Close()
			if ctx.Err() != nil {
				break
			}
			log.Printf("zone sync: controller %q election failed: %s", c.id, err)
			if !retryCluster(ctx) {
				break
			}
			continue
		}
		log.Printf("zone sync: controller %q elected leader (term %d)", c.id, election.Rev())
		leaderCtx, cancel := context.WithCancel(ctx)
		leaseDone := make(chan struct{})
		go func() {
			defer close(leaseDone)
			select {
			case <-session.Done():
				cancel()
			case <-leaderCtx.Done():
			}
		}()
		c.runLeader(leaderCtx, election.Key(), session.Lease())
		cancel()
		<-leaseDone
		c.demote()
		leaseLost := false
		select {
		case <-session.Done():
			leaseLost = true
		default:
		}
		if !leaseLost || ctx.Err() != nil {
			resignCtx, stopResign := context.WithTimeout(c.store.client.Ctx(), 2*time.Second)
			if err := election.Resign(resignCtx); err != nil && ctx.Err() == nil {
				log.Printf("zone sync: controller %q could not resign leadership: %s", c.id, err)
			}
			stopResign()
		}
		session.Close()
		log.Printf("zone sync: controller %q leadership ended", c.id)
		if !retryCluster(ctx) {
			break
		}
	}
	return nil
}

func retryCluster(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Second):
		return true
	}
}

func (c *Cluster) runLeader(ctx context.Context, leaderKey string, lease clientv3.LeaseID) {
	persistDone := make(chan struct{})
	go func() {
		defer close(persistDone)
		c.persistStatuses(ctx, leaderKey, lease)
	}()
	defer func() { <-persistDone }()
	for ctx.Err() == nil {
		s, revision, err := c.store.loadCurrent(ctx)
		if err == nil {
			if ctx.Err() != nil {
				return
			}
			if err := c.activate(ctx, s); err != nil {
				c.demote()
				log.Printf("zone sync: controller %q cannot load node statuses: %s", c.id, err)
				if !retryCluster(ctx) {
					return
				}
				continue
			}
		} else if errors.Is(err, ErrNoSnapshot) {
			c.demote()
			log.Printf("zone sync: controller %q is waiting for the first published snapshot", c.id)
		} else {
			c.demote()
			if ctx.Err() == nil {
				log.Printf("zone sync: controller %q cannot load published snapshot: %s", c.id, err)
			}
			if !retryCluster(ctx) {
				return
			}
			continue
		}
		watchCtx, cancelWatch := context.WithCancel(ctx)
		watch := c.store.client.Watch(watchCtx, c.store.currentKey(), clientv3.WithRev(revision+1))
		watchStopped := false
		changed := false
		for !watchStopped {
			select {
			case <-ctx.Done():
				cancelWatch()
				return
			case response, ok := <-watch:
				if !ok || response.Err() != nil {
					watchStopped = true
					break
				}
				if len(response.Events) > 0 {
					changed = true
					watchStopped = true
				}
			}
		}
		cancelWatch()
		if !changed && !retryCluster(ctx) {
			return
		}
	}
}

func (c *Cluster) activate(ctx context.Context, s snapshot) error {
	c.mu.RLock()
	needsStatusLoad := c.master == nil
	c.mu.RUnlock()
	var statuses map[string]NodeStatus
	if needsStatusLoad {
		var err error
		statuses, err = c.store.loadNodeStatuses(ctx)
		if err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.master == nil {
		master := newMaster(c.token, s)
		master.statuses = statuses
		c.master = master
		log.Printf("zone sync: controller %q serving revision %s", c.id, s.Revision)
		return nil
	}
	c.master.setSnapshot(s)
	return nil
}

func (c *Cluster) persistStatuses(ctx context.Context, leaderKey string, lease clientv3.LeaseID) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c.mu.RLock()
		master := c.master
		c.mu.RUnlock()
		if master == nil {
			continue
		}
		master.mu.Lock()
		statuses := make([]NodeStatus, 0, len(master.statuses))
		for _, status := range master.statuses {
			statuses = append(statuses, status)
		}
		master.mu.Unlock()
		if err := c.store.saveNodeStatuses(ctx, leaderKey, lease, statuses); err != nil && ctx.Err() == nil {
			log.Printf("zone sync: controller %q cannot persist node statuses: %s", c.id, err)
		}
	}
}

func (c *Cluster) demote() {
	c.mu.Lock()
	master := c.master
	c.master = nil
	c.mu.Unlock()
	if master != nil {
		master.closePeers()
	}
}

func (c *Cluster) activeMaster(w http.ResponseWriter) *Master {
	c.mu.RLock()
	master := c.master
	c.mu.RUnlock()
	if master == nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "controller is not the ready leader", http.StatusServiceUnavailable)
	}
	return master
}

func (c *Cluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if master := c.activeMaster(w); master != nil {
		master.ServeHTTP(w, r)
	}
}

func (c *Cluster) ServeStream(w http.ResponseWriter, r *http.Request) {
	if master := c.activeMaster(w); master != nil {
		master.ServeStream(w, r)
	}
}

func (c *Cluster) ServeNodes(w http.ResponseWriter, r *http.Request) {
	if master := c.activeMaster(w); master != nil {
		master.ServeNodes(w, r)
	}
}
