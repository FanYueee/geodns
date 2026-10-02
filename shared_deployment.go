package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/abh/geodns/v3/appconfig"
	"github.com/abh/geodns/v3/zonesync"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/gcfg.v1"
)

type deploymentSpec struct {
	Bootstrap string                                  `json:"bootstrap"`
	Nodes     map[string]*appconfig.ClusterNodeConfig `json:"nodes"`
}

type sharedDeployment struct {
	node *embeddedNode
	cfg  appconfig.AppConfig
	file string
}

func deploymentData(cfg appconfig.AppConfig) ([]byte, error) {
	if _, err := sharedNodeIDs(cfg); err != nil {
		return nil, err
	}
	spec := deploymentSpec{Bootstrap: cfg.Cluster.Bootstrap, Nodes: make(map[string]*appconfig.ClusterNodeConfig)}
	for id, value := range cfg.Node {
		node := *value
		node.Address, _ = clusterIP(node.Address)
		var ips []string
		for _, value := range strings.Split(node.Listen, ",") {
			ip, _ := clusterIP(value)
			ips = append(ips, ip)
		}
		node.Listen = strings.Join(ips, ",")
		spec.Nodes[id] = &node
	}
	return json.Marshal(spec)
}

func (d *sharedDeployment) key() string { return d.cfg.Controller.EtcdPrefix + "/deployment" }

func (d *sharedDeployment) saveBaseline(data []byte) error {
	if err := os.MkdirAll(d.node.config.Dir, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(d.node.config.Dir, ".deployment-local-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(d.node.config.Dir, "deployment-local.json"))
}

func (d *sharedDeployment) decode(data []byte) (deploymentSpec, error) {
	var spec deploymentSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return spec, err
	}
	cfg := d.cfg
	cfg.Node, cfg.Cluster.Bootstrap = spec.Nodes, spec.Bootstrap
	_, err := sharedNodeIDs(cfg)
	return spec, err
}

// New nodes may extend an existing list, but cannot overwrite settings or
// resurrect removed peers from a stale copy of the deployment file.
func (d *sharedDeployment) admit(ctx context.Context, client *clientv3.Client) error {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for requestCtx.Err() == nil {
		err := d.admitOnce(requestCtx, client)
		if !errors.Is(err, errDeploymentConflict) {
			return err
		}
	}
	return requestCtx.Err()
}

var errDeploymentConflict = errors.New("shared deployment changed concurrently")

func (d *sharedDeployment) admitOnce(requestCtx context.Context, client *clientv3.Client) error {
	data, err := deploymentData(d.cfg)
	if err != nil {
		return err
	}
	resp, err := client.Get(requestCtx, d.key())
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		result, err := client.Txn(requestCtx).If(clientv3.Compare(clientv3.CreateRevision(d.key()), "=", 0)).Then(clientv3.OpPut(d.key(), string(data))).Commit()
		if err != nil {
			return err
		}
		if !result.Succeeded {
			return errDeploymentConflict
		}
		return nil
	}
	spec, err := d.decode(resp.Kvs[0].Value)
	if err != nil {
		return err
	}
	if node := spec.Nodes[d.cfg.Cluster.ID]; node != nil {
		address, _ := clusterIP(d.cfg.Cluster.Address)
		if node.Address != address {
			return errors.New("shared node address differs from the published deployment")
		}
		return nil
	}
	if _, err := os.Stat(filepath.Join(d.node.config.Dir, "member", "wal")); err == nil {
		return errors.New("this node was removed from the deployment; use the current configuration and a fresh data directory to add it again")
	} else if !os.IsNotExist(err) {
		return err
	}
	localSpec, err := d.decode(data)
	if err != nil {
		return err
	}
	for id, existing := range spec.Nodes {
		local := localSpec.Nodes[id]
		if local == nil || *local != *existing {
			return errors.New("new node needs the current shared configuration; publish changes on an existing node first")
		}
	}
	if spec.Bootstrap != d.cfg.Cluster.Bootstrap {
		return errors.New("new node's bootstrap differs from the published deployment")
	}
	result, err := client.Txn(requestCtx).If(clientv3.Compare(clientv3.ModRevision(d.key()), "=", resp.Kvs[0].ModRevision)).Then(clientv3.OpPut(d.key(), string(data))).Commit()
	if err == nil && !result.Succeeded {
		return errDeploymentConflict
	}
	return err
}

// Shared settings are published on local file edits. Startup first uses the
// stored deployment, so an unchanged old file does not roll back live changes.
func (d *sharedDeployment) Run(ctx context.Context, controller *zonesync.Cluster) error {
	client, err := clientv3.New(clientv3.Config{Endpoints: d.node.endpoints(), DialTimeout: 5 * time.Second, AutoSyncInterval: 30 * time.Second})
	if err != nil {
		return err
	}
	defer client.Close()
	baseline, err := deploymentData(d.cfg)
	if err != nil {
		return err
	}
	fileBaseline, _ := os.ReadFile(d.file)
	localBaseline := baseline
	if saved, err := os.ReadFile(filepath.Join(d.node.config.Dir, "deployment-local.json")); err == nil {
		if _, err := d.decode(saved); err != nil {
			return fmt.Errorf("invalid saved local deployment baseline: %w", err)
		}
		if !bytes.Equal(saved, baseline) {
			// Detect offline edits without rolling back remote updates from an
			// unchanged stale local file.
			localBaseline, fileBaseline = saved, nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	var active deploymentSpec
	var revision int64
	for ctx.Err() == nil {
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := client.Txn(requestCtx).If(clientv3.Compare(clientv3.CreateRevision(d.key()), "=", 0)).Then(clientv3.OpPut(d.key(), string(baseline))).Else(clientv3.OpGet(d.key())).Commit()
		cancel()
		if err != nil {
			if !clusterRetry(ctx) {
				return nil
			}
			continue
		}
		data := baseline
		if !resp.Succeeded {
			data = resp.Responses[0].GetResponseRange().Kvs[0].Value
		}
		active, err = d.decode(data)
		if err != nil {
			return err
		}
		if active.Nodes[d.cfg.Cluster.ID] == nil {
			if err := d.admit(ctx, client); err != nil {
				return err
			}
			continue
		}
		revision = resp.Header.Revision
		break
	}
	if ctx.Err() != nil {
		return nil
	}
	apply := func(spec deploymentSpec) error {
		node := spec.Nodes[d.cfg.Cluster.ID]
		if node == nil {
			// Stay online until MemberRemove commits, so a two-voter cluster
			// can still form quorum while shrinking to one member.
			log.Printf("cluster: node %q awaiting removal following shared configuration", d.cfg.Cluster.ID)
			return nil
		}
		address, _ := clusterIP(d.cfg.Cluster.Address)
		if node.Address != address {
			return errors.New("a running node's WG address cannot change; remove it before creating a replacement")
		}
		return controller.SetWeight(node.Weight)
	}
	if err := apply(active); err != nil {
		return err
	}
	if fileBaseline != nil {
		if err := d.saveBaseline(localBaseline); err != nil {
			return fmt.Errorf("saving local deployment baseline: %w", err)
		}
	}
	log.Printf("cluster: node %q watching shared configuration %q", d.cfg.Cluster.ID, d.file)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	watch := client.Watch(clientv3.WithRequireLeader(ctx), d.key(), clientv3.WithRev(revision+1))
	var rejected []byte
	for {
		select {
		case <-ctx.Done():
			return nil
		case response, ok := <-watch:
			if !ok || response.Err() != nil {
				// A fresh read also recovers compacted watches.
				requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				resp, err := client.Get(requestCtx, d.key())
				cancel()
				if err != nil || len(resp.Kvs) == 0 {
					if !clusterRetry(ctx) {
						return nil
					}
					watch = nil
					continue
				}
				active, err = d.decode(resp.Kvs[0].Value)
				if err != nil {
					return err
				}
				if err := apply(active); err != nil {
					return err
				}
				revision = resp.Header.Revision
				watch = client.Watch(clientv3.WithRequireLeader(ctx), d.key(), clientv3.WithRev(resp.Header.Revision+1))
				continue
			}
			for _, event := range response.Events {
				if event.Type == clientv3.EventTypeDelete {
					return errors.New("shared deployment was deleted")
				}
				var err error
				active, err = d.decode(event.Kv.Value)
				if err != nil {
					return err
				}
				if err := apply(active); err != nil {
					return err
				}
				log.Printf("cluster: node %q applied shared deployment revision %d (%d nodes)", d.cfg.Cluster.ID, event.Kv.ModRevision, len(active.Nodes))
			}
			revision = response.Header.Revision
		case <-ticker.C:
			if watch == nil {
				// Retry the watch after temporary etcd unavailability.
				watch = client.Watch(clientv3.WithRequireLeader(ctx), d.key(), clientv3.WithRev(revision+1))
			}
			fileData, err := os.ReadFile(d.file)
			if err == nil && !bytes.Equal(fileData, fileBaseline) {
				requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				current, err := client.Get(requestCtx, d.key())
				cancel()
				var data, local []byte
				if err == nil && len(current.Kvs) != 0 {
					var latest deploymentSpec
					latest, err = d.decode(current.Kvs[0].Value)
					if err == nil {
						local, data, err = d.readFile(fileData, localBaseline, latest)
					}
				} else if err == nil {
					err = errors.New("shared deployment is missing")
				}
				if err == nil {
					requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if !bytes.Equal(current.Kvs[0].Value, data) {
						var result *clientv3.TxnResponse
						result, err = client.Txn(requestCtx).If(clientv3.Compare(clientv3.ModRevision(d.key()), "=", current.Kvs[0].ModRevision)).Then(clientv3.OpPut(d.key(), string(data))).Commit()
						if err == nil && !result.Succeeded {
							err = errors.New("shared deployment changed concurrently; retrying")
						}
					}
					cancel()
				}
				if err == nil {
					fileBaseline, rejected = fileData, nil
					localBaseline = local
					if err := d.saveBaseline(local); err != nil {
						return fmt.Errorf("saving local deployment baseline: %w", err)
					}
					log.Printf("cluster: node %q published shared configuration change", d.cfg.Cluster.ID)
				} else if !bytes.Equal(rejected, fileData) {
					log.Printf("cluster: shared configuration update rejected; keeping current deployment: %s", err)
					rejected = fileData
				}
			}
			if controller.IsLeader() {
				if err := d.reconcile(ctx, client); err != nil && ctx.Err() == nil {
					log.Printf("cluster: cannot reconcile shared membership yet: %s", err)
				}
			}
		}
	}
}

func (d *sharedDeployment) readFile(data, previous []byte, active deploymentSpec) ([]byte, []byte, error) {
	var cfg appconfig.AppConfig
	if err := gcfg.ReadInto(&cfg, bytes.NewReader(data)); err != nil {
		return nil, nil, err
	}
	c := cfg.Cluster
	original := d.cfg.Cluster
	defaults := func(value, fallback int) int {
		if value == 0 {
			return fallback
		}
		return value
	}
	name := c.Name
	if name == "" {
		name = "geodns"
	}
	mode, err := zonesync.ZoneMode(c.ZoneMode)
	originalMode, _ := zonesync.ZoneMode(original.ZoneMode)
	if err != nil || mode != originalMode {
		return nil, nil, errors.New("changing zone-mode requires restarting every controller")
	}
	if !c.Enabled || c.Token != original.Token || name != original.Name || defaults(c.ClientPort, 2379) != original.ClientPort || defaults(c.PeerPort, 2380) != original.PeerPort || defaults(c.SyncPort, 8053) != original.SyncPort || cfg.Sync != (appconfig.SyncConfig{}) || cfg.Controller != (appconfig.ControllerConfig{}) {
		return nil, nil, errors.New("shared edits may change node entries and bootstrap; cluster name, token, ports and mode require restart")
	}
	result, err := deploymentData(cfg)
	if err != nil {
		return nil, nil, err
	}
	var next deploymentSpec
	if err := json.Unmarshal(result, &next); err != nil {
		return nil, nil, err
	}
	prior, err := d.decode(previous)
	if err != nil {
		return nil, nil, err
	}
	merged := deploymentSpec{Bootstrap: active.Bootstrap, Nodes: make(map[string]*appconfig.ClusterNodeConfig)}
	for id, node := range active.Nodes {
		copy := *node
		merged.Nodes[id] = &copy
	}
	if next.Bootstrap != prior.Bootstrap {
		merged.Bootstrap = next.Bootstrap
	}
	// Apply only locally edited fields. An unchanged stale file must not delete
	// newer members or roll back weights published from another host.
	for id, old := range prior.Nodes {
		if next.Nodes[id] == nil {
			delete(merged.Nodes, id)
			continue
		}
		if next.Nodes[id].Address != old.Address {
			return nil, nil, fmt.Errorf("node %q WG address cannot change in place; remove it first", id)
		}
	}
	for id, node := range next.Nodes {
		old := prior.Nodes[id]
		current := merged.Nodes[id]
		if old != nil && *old == *node {
			continue
		}
		if current == nil {
			if old != nil {
				return nil, nil, fmt.Errorf("node %q was removed remotely; add it using the current deployment", id)
			}
			copy := *node
			merged.Nodes[id] = &copy
			continue
		}
		if node.Address != current.Address {
			return nil, nil, fmt.Errorf("node %q WG address conflicts with the published deployment", id)
		}
		if old == nil || node.Listen != old.Listen {
			current.Listen = node.Listen
		}
		if old == nil || node.Weight != old.Weight {
			current.Weight = node.Weight
		}
		if old == nil || node.ZoneDirectory != old.ZoneDirectory {
			current.ZoneDirectory = node.ZoneDirectory
		}
	}
	mergedData, err := json.Marshal(merged)
	if err == nil {
		_, err = d.decode(mergedData)
	}
	return result, mergedData, err
}

// Only the elected GeoDNS controller removes peers. New peers register on their
// own startup, so declaring an offline host does not increase quorum early.
func (d *sharedDeployment) reconcile(ctx context.Context, client *clientv3.Client) error {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := client.Get(requestCtx, d.key())
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return errors.New("shared deployment is missing")
	}
	spec, err := d.decode(resp.Kvs[0].Value)
	if err != nil {
		return err
	}
	wanted := map[string]bool{}
	for _, node := range spec.Nodes {
		wanted["http://"+net.JoinHostPort(node.Address, strconv.Itoa(d.cfg.Cluster.PeerPort))] = true
	}
	members, err := client.MemberList(requestCtx)
	if err != nil {
		return err
	}
	for _, member := range members.Members {
		keep := false
		for _, peer := range member.PeerURLs {
			keep = keep || wanted[peer]
		}
		if keep {
			continue
		}
		latest, err := client.Get(requestCtx, d.key())
		if err != nil {
			return err
		}
		if len(latest.Kvs) == 0 || latest.Kvs[0].ModRevision != resp.Kvs[0].ModRevision {
			return nil
		}
		if _, err := client.MemberRemove(requestCtx, member.ID); err != nil {
			return err
		}
		log.Printf("cluster: removed member %q (%x) following shared configuration", member.Name, member.ID)
		return nil
	}
	return nil
}
