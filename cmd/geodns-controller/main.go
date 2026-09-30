package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/abh/geodns/v3/appconfig"
	"github.com/abh/geodns/v3/applog"
	"github.com/abh/geodns/v3/zonesync"
	"golang.org/x/sync/errgroup"
)

func main() {
	configDir := flag.String("config", "./dns/", "directory of zone files")
	configFile := flag.String("configfile", "geodns.conf", "configuration file path (relative to -config unless absolute)")
	listen := flag.String("http", ":8053", "sync HTTP listen address")
	mode := flag.String("mode", "single", "controller mode: single or ha")
	etcdEndpoints := flag.String("etcd", "", "comma-separated etcd endpoints for HA mode")
	etcdPrefix := flag.String("etcd-prefix", "/geodns", "etcd key prefix for this cluster")
	etcdUser := flag.String("etcd-user", "", "etcd username")
	etcdPasswordFile := flag.String("etcd-password-file", "", "file containing the etcd password")
	etcdCA := flag.String("etcd-ca", "", "etcd CA certificate")
	etcdCert := flag.String("etcd-cert", "", "etcd client certificate")
	etcdKey := flag.String("etcd-key", "", "etcd client certificate key")
	id := flag.String("id", "", "unique controller ID in HA mode")
	publish := flag.Bool("publish", false, "publish the zone directory to HA storage and exit")
	logFile := flag.String("logfile", "", "log to file")
	checkConfig := flag.Bool("checkconfig", false, "check configuration and exit")
	flag.Parse()

	if *logFile != "" {
		applog.FileOpen(*logFile)
		defer applog.FileClose()
	}
	name := *configFile
	if !filepath.IsAbs(name) {
		name = filepath.Join(*configDir, name)
	}
	if err := appconfig.ConfigReader(name); err != nil {
		log.Fatal(err)
	}
	setFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })
	opts := controllerOptions{
		mode: *mode, listen: *listen, zoneDir: *configDir, id: *id,
		etcdEndpoints: *etcdEndpoints, etcdPrefix: *etcdPrefix, etcdUser: *etcdUser,
		etcdPasswordFile: *etcdPasswordFile, etcdCA: *etcdCA, etcdCert: *etcdCert, etcdKey: *etcdKey,
	}
	opts.applyConfig(appconfig.Config.Controller, name, setFlags)
	configuredMode := appconfig.Config.Sync.Mode
	if configuredMode != "controller" && !(opts.mode == "single" && configuredMode == "master") {
		log.Fatal("controller requires [sync] mode = controller (single mode also accepts master)")
	}
	switch opts.mode {
	case "single":
		if *publish || opts.etcdEndpoints != "" {
			log.Fatal("-publish and -etcd require -mode ha")
		}
		master, err := zonesync.NewMaster(opts.zoneDir, appconfig.Config.Sync.Token)
		if err != nil {
			log.Fatal(err)
		}
		if *checkConfig {
			return
		}
		serve(master, master.Run, opts.listen)
	case "ha":
		store, err := zonesync.DialStore(zonesync.EtcdOptions{
			Endpoints: opts.etcdEndpoints, Prefix: opts.etcdPrefix, Username: opts.etcdUser,
			PasswordFile: opts.etcdPasswordFile, CAFile: opts.etcdCA,
			CertFile: opts.etcdCert, KeyFile: opts.etcdKey,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer store.Close()
		if *publish {
			if *checkConfig {
				log.Fatal("-publish and -checkconfig cannot be used together")
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			revision, err := store.Publish(ctx, opts.zoneDir)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(revision)
			return
		}
		cluster, err := zonesync.NewCluster(store, opts.id, appconfig.Config.Sync.Token)
		if err != nil {
			log.Fatal(err)
		}
		if *checkConfig {
			return
		}
		serve(cluster, cluster.Run, opts.listen)
	default:
		log.Fatalf("unknown controller mode %q", opts.mode)
	}
}

type controllerOptions struct {
	mode, listen, zoneDir, id                   string
	etcdEndpoints, etcdPrefix, etcdUser         string
	etcdPasswordFile, etcdCA, etcdCert, etcdKey string
}

func (o *controllerOptions) applyConfig(c appconfig.ControllerConfig, configFile string, setFlags map[string]bool) {
	c = c.ResolvePaths(configFile)
	choose := func(flagName string, target *string, value string) {
		if !setFlags[flagName] && value != "" {
			*target = value
		}
	}
	choose("mode", &o.mode, c.Mode)
	choose("http", &o.listen, c.Listen)
	choose("config", &o.zoneDir, c.ZoneDirectory)
	choose("id", &o.id, c.ID)
	choose("etcd", &o.etcdEndpoints, c.EtcdEndpoints)
	choose("etcd-prefix", &o.etcdPrefix, c.EtcdPrefix)
	choose("etcd-user", &o.etcdUser, c.EtcdUser)
	choose("etcd-password-file", &o.etcdPasswordFile, c.EtcdPasswordFile)
	choose("etcd-ca", &o.etcdCA, c.EtcdCA)
	choose("etcd-cert", &o.etcdCert, c.EtcdCert)
	choose("etcd-key", &o.etcdKey, c.EtcdKey)
}

type syncHandler interface {
	http.Handler
	ServeStream(http.ResponseWriter, *http.Request)
	ServeNodes(http.ResponseWriter, *http.Request)
}

func serve(handler syncHandler, start func(context.Context) error, listen string) {
	if listen == "" {
		log.Fatal("-http is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, handler, start, listen); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, handler syncHandler, start func(context.Context) error, listen string) error {
	mux := http.NewServeMux()
	mux.Handle(zonesync.Path, handler)
	mux.HandleFunc(zonesync.StreamPath, handler.ServeStream)
	mux.HandleFunc(zonesync.NodesPath, handler.ServeNodes)
	server := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return start(ctx) })
	g.Go(func() error {
		log.Printf("zone sync: controller listening on %s", listen)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("controller HTTP server: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	})
	return g.Wait()
}
