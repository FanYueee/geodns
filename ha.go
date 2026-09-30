package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/abh/geodns/v3/appconfig"
	"github.com/abh/geodns/v3/zonesync"
)

type haNode struct {
	controller *zonesync.Cluster
	follower   *zonesync.Follower
	store      *zonesync.Store
	httpAddr   string
}

func newHANode(cfg appconfig.AppConfig, zoneDir, configFile, httpAddr string, httpExplicit bool) (*haNode, error) {
	if cfg.Sync.Mode != "ha" {
		return nil, errors.New("[sync] mode = ha is required")
	}
	if cfg.Sync.URLs == "" {
		return nil, errors.New("HA node requires [sync] urls with controller addresses")
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
	followerCfg := cfg.Sync
	followerCfg.Mode = "follower"
	_, follower, err := configureSync(followerCfg, zoneDir, httpAddr)
	if err != nil {
		return nil, err
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
	return &haNode{controller: cluster, follower: follower, store: store, httpAddr: httpAddr}, nil
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
