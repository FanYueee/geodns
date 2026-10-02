package zonesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Cluster serves sync requests only while this controller holds the etcd lease.
type Cluster struct {
	store         *Store
	id            string
	token         string
	leaseTTL      int
	weight        atomic.Int64
	weightChanged chan struct{}
	elected       atomic.Bool
	mu            sync.RWMutex
	master        *Master
	advertise     string
	zoneMode      string
	leadership    *leadership
}

type controllerAddress struct {
	ID     string `json:"id"`
	URL    string `json:"url"`
	Weight int    `json:"weight,omitempty"`
}

// SetWeight configures controller priority, including while Run is active. Equal weights
// retain the existing leader; only a strictly higher weight preempts it.
func (c *Cluster) SetWeight(weight int) error {
	if weight < 0 {
		return errors.New("controller weight must be zero or greater")
	}
	if c.weight.Swap(int64(weight)) != int64(weight) {
		select {
		case c.weightChanged <- struct{}{}:
		default:
		}
	}
	return nil
}

// IsLeader reports ownership of the election, even before the first Zone exists.
func (c *Cluster) IsLeader() bool { return c.elected.Load() }

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
	master, err := s.Leader(ctx)
	return master.URL, err
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
	return &Cluster{store: store, id: id, token: token, leaseTTL: 10, weightChanged: make(chan struct{}, 1)}, nil
}

func (c *Cluster) Run(ctx context.Context) error {
	select {
	case <-c.weightChanged:
	default:
	}
	defer c.demote()
	defer c.clearLeadership()
	defer c.elected.Store(false)
	for ctx.Err() == nil {
		err := c.store.configureZoneMode(ctx, c.zoneMode)
		if err != nil {
			if errors.Is(err, errAPIMode) {
				return err
			}
			if ctx.Err() == nil {
				log.Printf("zone sync: controller %q cannot initialize zone publication mode: %s", c.id, err)
			}
			if !retryCluster(ctx) {
				break
			}
			continue
		}
		weight := int(c.weight.Load())
		value := c.id
		if c.advertise != "" {
			data, err := json.Marshal(controllerAddress{ID: c.id, URL: c.advertise, Weight: weight})
			if err != nil {
				return err
			}
			value = string(data)
		}
		session, err := concurrency.NewSession(c.store.client, concurrency.WithTTL(c.leaseTTL), concurrency.WithContext(ctx))
		if err != nil {
			log.Printf("zone sync: controller %q cannot create election lease: %s", c.id, err)
			if !retryCluster(ctx) {
				break
			}
			continue
		}
		sessionCtx, cancel := context.WithCancel(ctx)
		leaseDone := make(chan struct{})
		go func() {
			defer close(leaseDone)
			select {
			case <-session.Done():
				cancel()
			case <-c.weightChanged:
				log.Printf("zone sync: controller %q weight changed to %d; renewing election", c.id, c.weight.Load())
				cancel()
			case <-sessionCtx.Done():
			}
		}()
		err = c.runCandidate(sessionCtx, session, value, weight)
		c.elected.Store(false)
		cancel()
		<-leaseDone
		c.demote()
		// Session.Close would revoke using the canceled process context on a
		// graceful shutdown, leaving priority registration until TTL expiry.
		// Stop keepalives, then revoke with the client's independent context.
		session.Orphan()
		revokeCtx, stopRevoke := context.WithTimeout(c.store.client.Ctx(), 2*time.Second)
		_, revokeErr := c.store.client.Revoke(revokeCtx, session.Lease())
		stopRevoke()
		if revokeErr != nil && ctx.Err() == nil {
			log.Printf("zone sync: controller %q could not revoke election lease; waiting for expiry: %s", c.id, revokeErr)
		}
		if err != nil && ctx.Err() == nil {
			log.Printf("zone sync: controller %q election session ended: %s", c.id, err)
		}
		if !retryCluster(ctx) {
			break
		}
	}
	return nil
}

// A candidacy lasts for the session, including while waiting or handing off.
// Keeping it on the same lease prevents low-weight nodes from reclaiming the
// election between a high-weight node's registration and its Campaign.
func (c *Cluster) runCandidate(ctx context.Context, session *concurrency.Session, value string, weight int) error {
	data, err := json.Marshal(controllerAddress{ID: c.id, URL: c.advertise, Weight: weight})
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s/candidates/%x", c.store.prefix, session.Lease())
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_, err = c.store.client.Put(requestCtx, key, string(data), clientv3.WithLease(session.Lease()))
	cancel()
	if err != nil {
		return err
	}
	log.Printf("zone sync: controller %q joined election with weight %d", c.id, weight)
	for ctx.Err() == nil {
		if err := c.waitPreference(ctx, true, weight); err != nil {
			return err
		}
		termCtx, stopTerm := context.WithCancel(ctx)
		preferenceDone := make(chan error, 1)
		go func() {
			// Fail closed if the candidate watch cannot be maintained.
			err := c.waitPreference(termCtx, false, weight)
			stopTerm()
			preferenceDone <- err
		}()
		election := concurrency.NewElection(session, c.store.prefix+"/election")
		err := election.Campaign(termCtx, value)
		if err == nil && termCtx.Err() == nil {
			c.mu.Lock()
			c.leadership = &leadership{ctx: termCtx, key: election.Key(), lease: session.Lease(), term: election.Rev()}
			c.mu.Unlock()
			c.elected.Store(true)
			log.Printf("zone sync: controller %q elected leader (weight %d, term %d)", c.id, weight, election.Rev())
			c.runLeader(termCtx, election.Key(), session.Lease())
		}
		stopTerm()
		c.clearLeadership()
		watchErr := <-preferenceDone
		c.elected.Store(false)
		// Stop serving and close streams BEFORE releasing the election key.
		c.demote()
		resignCtx, stopResign := context.WithTimeout(c.store.client.Ctx(), 2*time.Second)
		resignErr := election.Resign(resignCtx)
		stopResign()
		if resignErr != nil && ctx.Err() == nil {
			return fmt.Errorf("resigning election: %w", resignErr)
		}
		log.Printf("zone sync: controller %q leadership or pending campaign ended", c.id)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if watchErr != nil && !errors.Is(watchErr, context.Canceled) {
			return watchErr
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return ctx.Err()
}

// Use a linearizable read followed by a revision-based watch, so candidate
// arrival and lease expiry cannot be missed between checking and waiting.
func (c *Cluster) waitPreference(ctx context.Context, preferred bool, weight int) error {
	prefix := c.store.prefix + "/candidates/"
	for ctx.Err() == nil {
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := c.store.client.Get(requestCtx, prefix, clientv3.WithPrefix())
		cancel()
		if err != nil {
			return err
		}
		var higher *controllerAddress
		for _, kv := range resp.Kvs {
			var candidate controllerAddress
			if err := json.Unmarshal(kv.Value, &candidate); err != nil || !validNodeID(candidate.ID) || candidate.Weight < 0 || kv.Lease == 0 {
				return errors.New("invalid leased controller candidate")
			}
			if candidate.Weight > weight && (higher == nil || candidate.Weight > higher.Weight) {
				higher = &candidate
			}
		}
		if (higher == nil) == preferred {
			if higher != nil {
				log.Printf("zone sync: controller %q (weight %d) yielding election to %q (weight %d)", c.id, weight, higher.ID, higher.Weight)
			}
			return nil
		}
		watchCtx, cancelWatch := context.WithCancel(ctx)
		watch := c.store.client.Watch(clientv3.WithRequireLeader(watchCtx), prefix, clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
		select {
		case <-ctx.Done():
			cancelWatch()
			return ctx.Err()
		case response, ok := <-watch:
			cancelWatch()
			if !ok {
				return errors.New("controller candidate watch closed")
			}
			if err := response.Err(); err != nil {
				// Compaction is recovered with a fresh linearizable read.
				if response.CompactRevision == 0 {
					return err
				}
			}
		}
	}
	return ctx.Err()
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
	if c.zoneMode == "api" {
		c.ServeZoneSet(w, r)
		return
	}
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
