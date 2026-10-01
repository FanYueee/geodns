package zonesync

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdOptions struct {
	Endpoints        string
	Prefix           string
	Username         string
	PasswordFile     string
	CAFile           string
	CertFile         string
	KeyFile          string
	AutoSyncInterval time.Duration
}

func DialStore(o EtcdOptions) (*Store, error) {
	if o.Endpoints == "" {
		return nil, errors.New("etcd endpoints are required")
	}
	endpoints := strings.Split(o.Endpoints, ",")
	for i := range endpoints {
		endpoints[i] = strings.TrimSpace(endpoints[i])
		if endpoints[i] == "" {
			return nil, errors.New("empty etcd endpoint")
		}
	}
	config := clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second, Username: o.Username, AutoSyncInterval: o.AutoSyncInterval}
	if o.PasswordFile != "" {
		password, err := os.ReadFile(o.PasswordFile)
		if err != nil {
			return nil, err
		}
		config.Password = strings.TrimSpace(string(password))
	}
	if o.CAFile != "" || o.CertFile != "" || o.KeyFile != "" {
		tlsConfig, err := etcdTLS(o.CAFile, o.CertFile, o.KeyFile)
		if err != nil {
			return nil, err
		}
		config.TLS = tlsConfig
	}
	client, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(client, o.Prefix)
	if err != nil {
		client.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.client.Close() })
	return s.closeErr
}

// SetEndpoints switches to the local member once embedded etcd is ready.
func (s *Store) SetEndpoints(endpoints ...string) { s.client.SetEndpoints(endpoints...) }

func etcdTLS(caFile, certFile, keyFile string) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		data, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("no CA certificates in %s", caFile)
		}
		config.RootCAs = pool
	}
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("etcd certificate and key must be set together")
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{cert}
	}
	return config, nil
}
