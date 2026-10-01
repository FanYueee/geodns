package main

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/abh/geodns/v3/appconfig"
)

func configLocation(location, filename string, filenameExplicit bool) (string, string, error) {
	info, err := os.Stat(location)
	isFile := err == nil && !info.IsDir()
	if isFile || (err != nil && filepath.Ext(location) == ".conf") {
		if filenameExplicit {
			return "", "", errors.New("when -config points to a file, omit -configfile")
		}
		return filepath.Dir(location), filepath.Clean(location), nil
	}
	if filepath.IsAbs(filename) {
		return filepath.Clean(location), filename, nil
	}
	return filepath.Clean(location), filepath.Join(location, filename), nil
}

func dnsListenConfig(cfg appconfig.AppConfig, address, port string, addressExplicit, portExplicit bool) (string, string) {
	if !addressExplicit && cfg.DNS.Listen != "" {
		address = cfg.DNS.Listen
	}
	if !portExplicit && cfg.DNS.Port != "" {
		port = cfg.DNS.Port
	}
	return address, port
}
