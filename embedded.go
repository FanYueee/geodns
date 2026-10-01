package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/abh/geodns/v3/appconfig"
	"github.com/abh/geodns/v3/zonesync"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type embeddedNode struct {
	config    *embed.Config
	seeds     []string
	join      bool
	peerURL   string
	clientURL string
	store     *zonesync.Store
}

type embeddedLogWriter struct{}

func (embeddedLogWriter) Write(data []byte) (int, error) {
	log.Print(strings.TrimSpace(string(data)))
	return len(data), nil
}

func clusterIP(value string) (string, error) {
	ip, err := netip.ParseAddr(strings.TrimSpace(value))
	ip = ip.Unmap()
	if err != nil || ip.IsUnspecified() || ip.Zone() != "" {
		return "", fmt.Errorf("cluster address must be a concrete IP (normally a WireGuard IP): %q", value)
	}
	return ip.String(), nil
}

func validClusterID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, ch := range id {
		if ch != '-' && ch != '_' && ch != '.' && (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') {
			return false
		}
	}
	return true
}

func embeddedConfig(cfg appconfig.AppConfig, configFile string) (appconfig.AppConfig, *embeddedNode, error) {
	c := cfg.Cluster
	if !c.Enabled {
		return cfg, nil, nil
	}
	configFile, err := filepath.Abs(configFile)
	if err != nil {
		return cfg, nil, err
	}
	if cfg.Sync != (appconfig.SyncConfig{}) || cfg.Controller != (appconfig.ControllerConfig{}) {
		return cfg, nil, errors.New("[cluster] replaces [sync] and [controller]; use only one configuration style")
	}
	if !validClusterID(c.ID) || c.Token == "" {
		return cfg, nil, errors.New("[cluster] requires a unique id and shared token")
	}
	address, err := clusterIP(c.Address)
	if err != nil {
		return cfg, nil, err
	}
	ports := []*int{&c.ClientPort, &c.PeerPort, &c.SyncPort}
	for i, fallback := range []int{2379, 2380, 8053} {
		if *ports[i] == 0 {
			*ports[i] = fallback
		}
		if *ports[i] < 1 || *ports[i] > 65535 {
			return cfg, nil, errors.New("cluster ports must be between 1 and 65535")
		}
	}
	if c.ClientPort == c.PeerPort || c.ClientPort == c.SyncPort || c.PeerPort == c.SyncPort {
		return cfg, nil, errors.New("cluster client, peer, and sync ports must differ")
	}
	if (c.Members == "") == (c.Join == "") {
		return cfg, nil, errors.New("[cluster] requires either initial members or join addresses, not both")
	}
	if c.Name == "" {
		c.Name = "geodns"
	}
	if !validClusterID(c.Name) {
		return cfg, nil, errors.New("cluster name must contain 1-64 letters, digits, dots, underscores or hyphens")
	}
	resolve := func(value, fallback string) string {
		if value == "" {
			value = fallback
		}
		if !filepath.IsAbs(value) {
			value = filepath.Join(filepath.Dir(configFile), value)
		}
		return filepath.Clean(value)
	}
	dataDir := resolve(c.DataDirectory, "etcd-data")
	if c.ZoneDirectory != "" {
		c.ZoneDirectory = resolve(c.ZoneDirectory, "")
	}
	clientURL := "http://" + net.JoinHostPort(address, strconv.Itoa(c.ClientPort))
	peerURL := "http://" + net.JoinHostPort(address, strconv.Itoa(c.PeerPort))
	ec := embed.NewConfig()
	ec.Name, ec.Dir = c.ID, dataDir
	cu, _ := url.Parse(clientURL)
	pu, _ := url.Parse(peerURL)
	ec.ListenClientUrls, ec.AdvertiseClientUrls = []url.URL{*cu}, []url.URL{*cu}
	ec.ListenPeerUrls, ec.AdvertisePeerUrls = []url.URL{*pu}, []url.URL{*pu}
	ec.InitialClusterToken = c.Name
	ec.AutoCompactionMode, ec.AutoCompactionRetention = "periodic", "1h"
	ec.WarningUnaryRequestDuration = embed.DefaultWarningUnaryRequestDuration
	ec.LogLevel = "warn"
	// Send embedded server logs through GeoDNS's logger, including -logfile.
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.Lock(zapcore.AddSync(embeddedLogWriter{})), zap.WarnLevel)
	core = core.With([]zapcore.Field{zap.String("node", c.ID)})
	ec.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.New(core))
	n := &embeddedNode{config: ec, join: c.Join != "", peerURL: peerURL, clientURL: clientURL}
	var initial []string
	if n.join {
		ec.ClusterState = embed.ClusterStateFlagExisting
		ec.InitialCluster = c.ID + "=" + peerURL
		for _, seed := range strings.Split(c.Join, ",") {
			ip, err := clusterIP(seed)
			if err != nil {
				return cfg, nil, err
			}
			if ip == address {
				return cfg, nil, errors.New("join must point to existing nodes, not this node")
			}
			n.seeds = append(n.seeds, "http://"+net.JoinHostPort(ip, strconv.Itoa(c.ClientPort)))
		}
	} else {
		ids, addresses := map[string]bool{}, map[string]bool{}
		for _, entry := range strings.Split(c.Members, ",") {
			id, ipText, ok := strings.Cut(strings.TrimSpace(entry), "=")
			id = strings.TrimSpace(id)
			ip, err := clusterIP(ipText)
			if !ok || !validClusterID(id) || err != nil || ids[id] || addresses[ip] {
				return cfg, nil, fmt.Errorf("invalid or duplicate cluster member %q; use id=IP entries", entry)
			}
			ids[id], addresses[ip] = true, true
			if id == c.ID && ip != address {
				return cfg, nil, errors.New("this node's address differs from its members entry")
			}
			initial = append(initial, id+"=http://"+net.JoinHostPort(ip, strconv.Itoa(c.PeerPort)))
			n.seeds = append(n.seeds, "http://"+net.JoinHostPort(ip, strconv.Itoa(c.ClientPort)))
		}
		if !ids[c.ID] {
			return cfg, nil, errors.New("initial members must include this node; use join to add a node to an existing cluster")
		}
		ec.InitialCluster = strings.Join(initial, ",")
	}
	if err := ec.Validate(); err != nil {
		return cfg, nil, err
	}
	cfg.Sync = appconfig.SyncConfig{Mode: "ha", ID: c.ID, Token: c.Token}
	cfg.Controller = appconfig.ControllerConfig{
		Listen:        net.JoinHostPort(address, strconv.Itoa(c.SyncPort)),
		ZoneDirectory: c.ZoneDirectory, EtcdEndpoints: strings.Join(n.seeds, ","),
		EtcdPrefix: "/geodns/" + c.Name,
	}
	cfg.Cluster = c
	return cfg, n, nil
}

func (n *embeddedNode) Run(ctx context.Context) error {
	var learnerID uint64
	if n.join {
		_, err := os.Stat(filepath.Join(n.config.Dir, "member", "wal"))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if os.IsNotExist(err) {
			var err error
			for ctx.Err() == nil {
				learnerID, err = n.register(ctx)
				if err == nil {
					break
				}
				log.Printf("cluster: node %q cannot join yet: %s; retrying", n.config.Name, err)
				if !clusterRetry(ctx) {
					return nil
				}
			}
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	e, err := embed.StartEtcd(n.config)
	if err != nil {
		return fmt.Errorf("embedded etcd: %w", err)
	}
	defer e.Close()
	log.Printf("cluster: embedded etcd node %q started; data directory %q", n.config.Name, n.config.Dir)
	select {
	case <-ctx.Done():
		return nil
	case err := <-e.Err():
		return fmt.Errorf("embedded etcd startup: %v", err)
	case <-e.Server.ReadyNotify():
	}
	log.Printf("cluster: embedded etcd node %q is ready", n.config.Name)
	if n.join {
		// Check on every restart as a previous join may have stopped before promotion.
		if err := n.promote(ctx, learnerID); err != nil {
			return err
		}
	}
	if n.store != nil {
		n.store.SetEndpoints(n.clientURL)
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-e.Err():
		return fmt.Errorf("embedded etcd: %v", err)
	case <-e.Server.StopNotify():
		return errors.New("embedded etcd stopped (the member may have been removed)")
	}
}

func clusterRetry(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (n *embeddedNode) client() (*clientv3.Client, error) {
	endpoints := n.endpoints()
	return clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
}

func (n *embeddedNode) endpoints() []string {
	endpoints := append([]string{}, n.seeds...)
	for _, endpoint := range endpoints {
		if endpoint == n.clientURL {
			return endpoints
		}
	}
	return append(endpoints, n.clientURL)
}

func (n *embeddedNode) register(ctx context.Context) (uint64, error) {
	client, err := n.client()
	if err != nil {
		return 0, err
	}
	defer client.Close()
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, err := client.MemberList(requestCtx)
	if err != nil {
		return 0, err
	}
	var own *etcdserverpb.Member
	for _, member := range response.Members {
		if len(member.PeerURLs) == 1 && member.PeerURLs[0] == n.peerURL {
			if member.Name != "" && member.Name != n.config.Name {
				return 0, errors.New("join address is already used by another member")
			}
			own = member
		} else if member.Name == n.config.Name {
			return 0, errors.New("node id is already used at a different address")
		}
	}
	if own == nil {
		added, err := client.MemberAddAsLearner(requestCtx, []string{n.peerURL})
		if err != nil {
			return 0, err
		}
		response.Members, own = added.Members, added.Member
		log.Printf("cluster: registered node %q as learner %x", n.config.Name, own.ID)
	}
	var initial []string
	for _, member := range response.Members {
		name := member.Name
		if member.ID == own.ID {
			name = n.config.Name
		}
		if name == "" {
			return 0, errors.New("another cluster member is still starting")
		}
		for _, peerURL := range member.PeerURLs {
			initial = append(initial, name+"="+peerURL)
		}
	}
	n.config.InitialCluster = strings.Join(initial, ",")
	return own.ID, nil
}

func (n *embeddedNode) promote(ctx context.Context, id uint64) error {
	client, err := n.client()
	if err != nil {
		return err
	}
	defer client.Close()
	for ctx.Err() == nil {
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		members, err := client.MemberList(requestCtx)
		if err == nil {
			var own *etcdserverpb.Member
			for _, member := range members.Members {
				if (member.Name == "" || member.Name == n.config.Name) && len(member.PeerURLs) == 1 && member.PeerURLs[0] == n.peerURL {
					own = member
					break
				}
			}
			if own == nil {
				cancel()
				return fmt.Errorf("embedded etcd member %q no longer belongs to the cluster", n.config.Name)
			}
			if id != 0 && own.ID != id {
				cancel()
				return errors.New("joined member identity changed before promotion")
			}
			if !own.IsLearner {
				cancel()
				return nil
			}
			_, err = client.MemberPromote(requestCtx, own.ID)
		}
		cancel()
		if err == nil {
			log.Printf("cluster: node %q promoted to voting member", n.config.Name)
			return nil
		}
		log.Printf("cluster: node %q waiting for learner promotion: %s", n.config.Name, err)
		if !clusterRetry(ctx) {
			break
		}
	}
	return nil
}
