package main

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"

	"github.com/abh/geodns/v3/appconfig"
)

func sharedNodeIDs(cfg appconfig.AppConfig) ([]string, error) {
	if len(cfg.Node) == 0 {
		return nil, errors.New("shared cluster configuration requires node entries")
	}
	ids := make([]string, 0, len(cfg.Node))
	addresses := map[string]bool{}
	for id, node := range cfg.Node {
		if !validClusterID(id) || node == nil {
			return nil, fmt.Errorf("invalid shared node %q", id)
		}
		address, err := clusterIP(node.Address)
		if err != nil || addresses[address] {
			return nil, fmt.Errorf("node %q needs a unique concrete WG address", id)
		}
		addresses[address] = true
		if node.Weight < 0 || node.Listen == "" {
			return nil, fmt.Errorf("node %q requires DNS listen addresses and a nonnegative weight", id)
		}
		for _, value := range strings.Split(node.Listen, ",") {
			if _, err := clusterIP(value); err != nil {
				return nil, fmt.Errorf("node %q DNS listen: %w", id, err)
			}
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if cfg.Node[cfg.Cluster.Bootstrap] == nil {
		return nil, errors.New("cluster bootstrap must name one of the shared nodes")
	}
	if cfg.Cluster.Members != "" || cfg.Cluster.Join != "" {
		return nil, errors.New("shared node entries replace members and join")
	}
	return ids, nil
}

func localClusterIPs() (map[string]bool, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	ips := map[string]bool{}
	for _, address := range addresses {
		prefix, err := netip.ParsePrefix(address.String())
		if err == nil {
			ips[prefix.Addr().Unmap().String()] = true
		}
	}
	return ips, nil
}

// Shared files are identical on all hosts. ID is only an optional explicit
// override for hosts running more than one node (and for local integration tests).
func resolveSharedConfig(cfg appconfig.AppConfig) (appconfig.AppConfig, error) {
	if !cfg.Cluster.Enabled || len(cfg.Node) == 0 {
		return cfg, nil
	}
	if _, err := sharedNodeIDs(cfg); err != nil {
		return cfg, err
	}
	id := cfg.Cluster.ID
	if id == "" {
		ips, err := localClusterIPs()
		if err != nil {
			return cfg, err
		}
		for candidate, node := range cfg.Node {
			address, _ := clusterIP(node.Address)
			if ips[address] {
				if id != "" {
					return cfg, errors.New("multiple shared nodes match local interfaces; set cluster id to select one")
				}
				id = candidate
			}
		}
	}
	node := cfg.Node[id]
	if node == nil {
		return cfg, errors.New("no shared node matches this host's WG IP; configure its interface first or set cluster id")
	}
	cfg.Cluster.ID, cfg.Cluster.Address, cfg.Cluster.Weight = id, node.Address, node.Weight
	cfg.Cluster.ZoneDirectory = node.ZoneDirectory
	cfg.DNS.Listen = node.Listen
	return cfg, nil
}
