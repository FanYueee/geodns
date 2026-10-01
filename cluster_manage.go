package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/abh/geodns/v3/appconfig"
	"github.com/abh/geodns/v3/zonesync"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func manageCluster(ctx context.Context, cfg appconfig.AppConfig, configFile, remove string, output io.Writer) error {
	if !cfg.Cluster.Enabled {
		return errors.New("cluster management requires [cluster] enabled = true")
	}
	cfg, node, err := embeddedConfig(cfg, configFile)
	if err != nil {
		return err
	}
	if remove == cfg.Cluster.ID {
		return errors.New("use another node's config to remove this member; stop the target node first")
	}
	client, err := node.client()
	if err != nil {
		return err
	}
	defer client.Close()
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	members, err := client.MemberList(requestCtx)
	if err != nil {
		return err
	}
	if remove != "" {
		var target *etcdserverpb.Member
		for _, member := range members.Members {
			if member.Name == remove {
				target = member
				break
			}
		}
		if target == nil {
			id, err := strconv.ParseUint(strings.TrimPrefix(remove, "0x"), 16, 64)
			if err == nil {
				for _, member := range members.Members {
					if member.ID == id {
						target = member
						break
					}
				}
			}
		}
		if target == nil {
			return fmt.Errorf("cluster member %q not found", remove)
		}
		if target.Name == cfg.Cluster.ID {
			return errors.New("use another node's config to remove this member; stop the target node first")
		}
		for _, peerURL := range target.PeerURLs {
			if peerURL == node.peerURL {
				return errors.New("use another node's config to remove this member; stop the target node first")
			}
		}
		if _, err := client.MemberRemove(requestCtx, target.ID); err != nil {
			return err
		}
		_, err := fmt.Fprintf(output, "Removed cluster member %s (%x). Keep its old data directory; do not restart it as a new cluster.\n", remove, target.ID)
		return err
	}
	sort.Slice(members.Members, func(i, j int) bool { return members.Members[i].Name < members.Members[j].Name })
	table := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "NODE\tMEMBER-ID\tROLE\tPEER\tCLIENT"); err != nil {
		return err
	}
	for _, member := range members.Members {
		role := "voter"
		if member.IsLearner {
			role = "learner"
		}
		name := member.Name
		if name == "" {
			name = fmt.Sprintf("starting-%x", member.ID)
		}
		if _, err := fmt.Fprintf(table, "%s\t%x\t%s\t%s\t%s\n", name, member.ID, role, strings.Join(member.PeerURLs, ","), strings.Join(member.ClientURLs, ",")); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	store, err := zonesync.NewStore(client, cfg.Controller.EtcdPrefix)
	if err != nil {
		return err
	}
	leader, err := store.LeaderURL(requestCtx)
	if err != nil {
		_, err = fmt.Fprintf(output, "GeoDNS controller: unavailable (%s)\n", err)
		return err
	}
	_, err = fmt.Fprintf(output, "GeoDNS controller: %s\n", leader)
	return err
}
