package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/abh/geodns/v3/appconfig"
	"github.com/abh/geodns/v3/zonesync"
	"golang.org/x/sync/errgroup"
)

type haNode struct {
	controller   *zonesync.Cluster
	follower     *zonesync.Follower
	store        *zonesync.Store
	httpAddr     string
	bootstrapDir string
}

func newHANode(cfg appconfig.AppConfig, zoneDir, configFile, httpAddr string, httpExplicit bool) (*haNode, error) {
	if cfg.Sync.Mode != "ha" {
		return nil, errors.New("[sync] mode = ha is required")
	}
	if cfg.Sync.Interval != "" {
		return nil, errors.New("sync interval is no longer used; remove it from HA configuration")
	}
	if cfg.Sync.URL != "" {
		return nil, errors.New("HA node uses automatic discovery or explicit sync urls, not url")
	}
	controller := cfg.Controller.ResolvePaths(configFile)
	if controller.Mode != "" && controller.Mode != "ha" {
		return nil, fmt.Errorf("[controller] mode must be ha, got %q", controller.Mode)
	}
	if controller.ZoneDirectory != "" {
		source, err := filepath.Abs(controller.ZoneDirectory)
		if err != nil {
			return nil, err
		}
		cache, err := filepath.Abs(zoneDir)
		if err != nil {
			return nil, err
		}
		same := source == cache
		if !same {
			sourceInfo, sourceErr := os.Stat(source)
			cacheInfo, cacheErr := os.Stat(cache)
			same = sourceErr == nil && cacheErr == nil && os.SameFile(sourceInfo, cacheInfo)
		}
		if same {
			return nil, errors.New("[controller] zone-directory must differ from the node's -config directory")
		}
	}
	if !httpExplicit {
		if controller.Listen == "" {
			return nil, errors.New("HA node requires [controller] listen or -http")
		}
		httpAddr = controller.Listen
	}
	if httpAddr == "" {
		return nil, errors.New("HA node requires an HTTP listener")
	}
	store, err := dialControllerStore(controller)
	if err != nil {
		return nil, err
	}
	id := controller.ID
	if id == "" {
		id = cfg.Sync.ID
	}
	cluster, err := zonesync.NewCluster(store, id, cfg.Sync.Token)
	if err != nil {
		store.Close()
		return nil, err
	}
	advertise := controller.Advertise
	if advertise == "" {
		advertise = "http://" + httpAddr
	}
	if err := cluster.SetAdvertiseURL(advertise); err != nil {
		if controller.Advertise != "" || cfg.Sync.URLs == "" {
			store.Close()
			return nil, fmt.Errorf("invalid controller advertise address (use a reachable WG address, or set advertise): %w", err)
		}
	}
	var follower *zonesync.Follower
	if cfg.Sync.URLs == "" {
		follower, err = zonesync.NewDiscoveredFollower(zoneDir, cfg.Sync.Token, cfg.Sync.ID, store)
	} else {
		followerCfg := cfg.Sync
		followerCfg.Mode = "follower"
		_, follower, err = configureSync(followerCfg, zoneDir, httpAddr)
	}
	if err != nil {
		store.Close()
		return nil, err
	}
	return &haNode{controller: cluster, follower: follower, store: store, httpAddr: httpAddr, bootstrapDir: controller.ZoneDirectory}, nil
}

func (n *haNode) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return n.controller.Run(ctx) })
	if n.bootstrapDir != "" {
		g.Go(func() error {
			for ctx.Err() == nil {
				revision, err := n.store.Bootstrap(ctx, n.bootstrapDir)
				if err == nil {
					log.Printf("zone sync: bootstrap ready; cluster revision %s", revision)
					return nil
				}
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("zone sync: bootstrap failed: %s", err)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(5 * time.Second):
				}
			}
			return nil
		})
	}
	return g.Wait()
}

func dialControllerStore(c appconfig.ControllerConfig) (*zonesync.Store, error) {
	prefix := c.EtcdPrefix
	if prefix == "" {
		prefix = "/geodns"
	}
	return zonesync.DialStore(zonesync.EtcdOptions{
		Endpoints: c.EtcdEndpoints, Prefix: prefix, Username: c.EtcdUser,
		PasswordFile: c.EtcdPasswordFile, CAFile: c.EtcdCA,
		CertFile: c.EtcdCert, KeyFile: c.EtcdKey,
	})
}

func publishHANode(ctx context.Context, cfg appconfig.AppConfig, configFile string) (string, error) {
	if cfg.Sync.Mode != "ha" {
		return "", errors.New("-publish requires [sync] mode = ha")
	}
	controller := cfg.Controller.ResolvePaths(configFile)
	if controller.Mode != "" && controller.Mode != "ha" {
		return "", errors.New("-publish requires [controller] mode = ha")
	}
	if controller.ZoneDirectory == "" {
		return "", errors.New("-publish requires [controller] zone-directory")
	}
	store, err := dialControllerStore(controller)
	if err != nil {
		return "", err
	}
	defer store.Close()
	return store.Publish(ctx, controller.ZoneDirectory)
}
