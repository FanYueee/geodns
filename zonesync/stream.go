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
	"sort"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
)

const (
	heartbeatInterval = 5 * time.Second
	heartbeatTimeout  = 15 * time.Second
	reconnectMax      = 30 * time.Second
)

type NodeStatus struct {
	ID              string    `json:"id"`
	Connected       bool      `json:"connected"`
	LastSeen        time.Time `json:"last_seen"`
	AppliedRevision string    `json:"applied_revision,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
}

type acknowledgement struct {
	Revision string `json:"revision"`
	Error    string `json:"error,omitempty"`
}

type peer struct {
	id      string
	remote  string
	conn    *websocket.Conn
	updates chan snapshot
}

func validNodeID(id string) bool {
	return len(id) <= 64 && zoneFile(id+".json")
}

// Run watches the master directory and broadcasts a complete revision on change.
func (m *Master) Run(ctx context.Context) error {
	defer m.closePeers()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()
	if err := watcher.Add(m.dir); err != nil {
		return err
	}
	if err := m.publish(); err != nil {
		return err
	}

	// The periodic scan recovers events lost by a file watcher overflow. Nodes
	// still receive data only when the content revision changes.
	reconcile := time.NewTicker(time.Minute)
	defer reconcile.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return errors.New("zone directory watcher stopped")
			}
			if !strings.HasSuffix(strings.ToLower(event.Name), ".json") {
				continue
			}
			// Wait for an atomic rename or burst of writes to settle.
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return nil
			}
			if err := m.publish(); err != nil {
				log.Printf("zone sync: master snapshot failed: %s", err)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return errors.New("zone directory watcher stopped")
			}
			log.Printf("zone sync: master directory watcher: %s", err)
		case <-reconcile.C:
			if err := m.publish(); err != nil {
				log.Printf("zone sync: master snapshot failed: %s", err)
			}
		}
	}
}

func (m *Master) publish() error {
	s, err := readSnapshot(m.dir)
	if err != nil {
		return err
	}
	m.setSnapshot(s)
	return nil
}

func (m *Master) setSnapshot(s snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.latest.Revision == s.Revision {
		return
	}
	m.latest = s
	for _, p := range m.peers {
		select {
		case p.updates <- s:
		default:
			<-p.updates
			p.updates <- s
		}
	}
	log.Printf("zone sync: published revision %s to %d nodes", s.Revision, len(m.peers))
}

func (m *Master) closePeers() {
	m.mu.Lock()
	m.accepting = false
	for _, p := range m.peers {
		p.conn.Close()
	}
	m.mu.Unlock()
}

func (m *Master) ServeNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !m.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	m.mu.Lock()
	statuses := make([]NodeStatus, 0, len(m.statuses))
	for _, status := range m.statuses {
		statuses = append(statuses, status)
	}
	m.mu.Unlock()
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].ID < statuses[j].ID })
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(statuses); err != nil {
		log.Printf("zone sync: node status response: %s", err)
	}
}

var upgrader = websocket.Upgrader{}

func (m *Master) ServeStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !m.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := r.URL.Query().Get("id")
	if !validNodeID(id) {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("zone sync: upgrade for node %q failed: %s", id, err)
		return
	}
	p := &peer{id: id, remote: r.RemoteAddr, conn: conn, updates: make(chan snapshot, 1)}
	m.mu.Lock()
	if !m.accepting {
		m.mu.Unlock()
		conn.Close()
		return
	}
	previous := m.peers[id]
	status, seenBefore := m.statuses[id]
	m.peers[id] = p
	status.ID = id
	status.Connected = true
	status.LastSeen = time.Now()
	status.LastError = ""
	m.statuses[id] = status
	p.updates <- m.latest
	m.mu.Unlock()
	if previous != nil {
		previous.conn.Close()
		log.Printf("zone sync: node %q replaced connection from %s with %s", id, previous.remote, p.remote)
	} else if seenBefore {
		log.Printf("zone sync: node %q reconnected from %s", id, p.remote)
	} else {
		log.Printf("zone sync: node %q connected from %s", id, p.remote)
	}
	p.serve(m)
}

func (p *peer) serve(m *Master) {
	defer p.conn.Close()
	done := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		err := p.writeLoop(done)
		if err != nil {
			p.conn.Close()
		}
		writeDone <- err
	}()
	p.conn.SetReadLimit(16 << 10)
	p.conn.SetReadDeadline(time.Now().Add(heartbeatTimeout))
	p.conn.SetPongHandler(func(string) error {
		p.conn.SetReadDeadline(time.Now().Add(heartbeatTimeout))
		m.mu.Lock()
		if m.peers[p.id] == p {
			status := m.statuses[p.id]
			status.LastSeen = time.Now()
			m.statuses[p.id] = status
		}
		m.mu.Unlock()
		return nil
	})
	var disconnectErr error
	for {
		var ack acknowledgement
		if err := p.conn.ReadJSON(&ack); err != nil {
			disconnectErr = err
			break
		}
		if ack.Revision == "" {
			continue
		}
		m.mu.Lock()
		if m.peers[p.id] == p {
			status := m.statuses[p.id]
			status.LastSeen = time.Now()
			if ack.Error == "" {
				status.AppliedRevision = ack.Revision
				status.LastError = ""
			} else {
				status.LastError = ack.Error
			}
			m.statuses[p.id] = status
		}
		m.mu.Unlock()
		if ack.Error != "" {
			log.Printf("zone sync: node %q failed to apply revision %s: %s", p.id, ack.Revision, ack.Error)
		} else {
			log.Printf("zone sync: node %q applied revision %s", p.id, ack.Revision)
		}
	}
	close(done)
	p.conn.Close()
	reason := fmt.Sprintf("connection closed: %s", disconnectErr)
	var networkErr net.Error
	if errors.As(disconnectErr, &networkErr) && networkErr.Timeout() {
		reason = "heartbeat timed out"
	}
	if err := <-writeDone; err != nil {
		reason = fmt.Sprintf("send failed: %s", err)
	}
	m.mu.Lock()
	var lastSeen time.Time
	current := m.peers[p.id] == p
	if current {
		delete(m.peers, p.id)
		status := m.statuses[p.id]
		status.Connected = false
		status.LastError = reason
		m.statuses[p.id] = status
		lastSeen = status.LastSeen
	}
	m.mu.Unlock()
	if current {
		log.Printf("zone sync: node %q disconnected from %s; %s; last_seen=%s", p.id, p.remote, reason, lastSeen.Format(time.RFC3339))
	}
}

func (p *peer) writeLoop(done <-chan struct{}) error {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return nil
		case s := <-p.updates:
			p.conn.SetWriteDeadline(time.Now().Add(heartbeatTimeout))
			if err := p.conn.WriteJSON(s); err != nil {
				return err
			}
		case <-ticker.C:
			if err := p.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(heartbeatInterval)); err != nil {
				return err
			}
		}
	}
}

func (f *Follower) Run(ctx context.Context) {
	backoff := time.Second
	retrying := false
	for ctx.Err() == nil {
		connected, err := f.stream(ctx, retrying)
		if ctx.Err() != nil {
			return
		}
		retrying = true
		if connected {
			log.Printf("zone sync: node %q disconnected from master: %s", f.id, err)
			backoff = time.Second
		} else {
			log.Printf("zone sync: node %q could not connect to master: %s", f.id, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < reconnectMax {
			backoff *= 2
			if backoff > reconnectMax {
				backoff = reconnectMax
			}
		}
	}
}

func (f *Follower) stream(ctx context.Context, retrying bool) (bool, error) {
	urls := f.urls
	if f.discover != nil {
		origin, err := f.discover(ctx)
		if err != nil {
			return false, fmt.Errorf("discover leader: %w", err)
		}
		urls = []string{origin}
	}
	header := http.Header{"Authorization": []string{"Bearer " + f.token}}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	var conn *websocket.Conn
	var masterHost string
	var failures []error
	for offset := range urls {
		index := (f.nextURL + offset) % len(urls)
		u, err := url.Parse(urls[index])
		if err != nil {
			return false, err
		}
		if u.Scheme == "https" {
			u.Scheme = "wss"
		} else {
			u.Scheme = "ws"
		}
		u.Path = StreamPath
		u.RawQuery = "id=" + url.QueryEscape(f.id)
		candidate, resp, err := dialer.DialContext(ctx, u.String(), header)
		if err != nil {
			if resp != nil {
				resp.Body.Close()
				failures = append(failures, fmt.Errorf("%s returned HTTP %d: %w", u.Host, resp.StatusCode, err))
			} else {
				failures = append(failures, fmt.Errorf("%s: %w", u.Host, err))
			}
			continue
		}
		conn = candidate
		masterHost = u.Host
		f.nextURL = (index + 1) % len(urls)
		break
	}
	if conn == nil {
		return false, errors.Join(failures...)
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if retrying {
		log.Printf("zone sync: node %q reconnected to master %s", f.id, masterHost)
	} else {
		log.Printf("zone sync: node %q connected to master %s", f.id, masterHost)
	}
	conn.SetReadLimit(maxSnapshotSize + 1<<20)
	conn.SetReadDeadline(time.Now().Add(heartbeatTimeout))
	defaultPingHandler := conn.PingHandler()
	conn.SetPingHandler(func(data string) error {
		conn.SetReadDeadline(time.Now().Add(heartbeatTimeout))
		return defaultPingHandler(data)
	})
	snapshots := make(chan snapshot, 1)
	readErrors := make(chan error, 1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			var s snapshot
			if err := conn.ReadJSON(&s); err != nil {
				readErrors <- err
				return
			}
			select {
			case snapshots <- s:
			default:
				<-snapshots
				snapshots <- s
			}
		}
	}()
	defer func() {
		conn.Close()
		<-readDone
	}()
	for {
		var s snapshot
		select {
		case s = <-snapshots:
		case err := <-readErrors:
			var networkErr net.Error
			if errors.As(err, &networkErr) && networkErr.Timeout() {
				return true, fmt.Errorf("master heartbeat timed out: %w", err)
			}
			return true, err
		case <-ctx.Done():
			return true, ctx.Err()
		}
		var applyErr error
		if s.Version != 1 || s.Revision == "" || s.Zones == nil {
			applyErr = errors.New("unsupported zone snapshot")
		} else {
			applyErr = f.apply(s)
			if applyErr == nil && f.reload != nil {
				applyErr = f.reload()
			}
		}
		ack := acknowledgement{Revision: s.Revision}
		if applyErr != nil {
			ack.Error = applyErr.Error()
			log.Printf("zone sync: node %q failed to apply revision %s: %s", f.id, s.Revision, applyErr)
		} else {
			log.Printf("zone sync: node %q applied revision %s", f.id, s.Revision)
		}
		conn.SetWriteDeadline(time.Now().Add(heartbeatTimeout))
		if err := conn.WriteJSON(ack); err != nil {
			return true, err
		}
	}
}
