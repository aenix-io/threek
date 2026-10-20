// Package etcdconf resolves the etcd settings the agent needs, following
// etcd's own precedence rules.
package etcdconf

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/aenix-io/threek/internal/flagscan"
)

const defaultClientURL = "http://localhost:2379"

// Settings is the subset of etcd configuration the agent uses.
type Settings struct {
	CertFile            string
	KeyFile             string
	TrustedCAFile       string
	ListenClientURLs    []string
	AdvertiseClientURLs []string
}

// fileConfig mirrors the keys etcd reads from --config-file.
type fileConfig struct {
	ListenClientURLs    string `json:"listen-client-urls"`
	AdvertiseClientURLs string `json:"advertise-client-urls"`
	ClientSecurity      struct {
		CertFile      string `json:"cert-file"`
		KeyFile       string `json:"key-file"`
		TrustedCAFile string `json:"trusted-ca-file"`
	} `json:"client-transport-security"`
}

var valueFlags = map[string]bool{
	"config-file":           true,
	"cert-file":             true,
	"key-file":              true,
	"trusted-ca-file":       true,
	"listen-client-urls":    true,
	"advertise-client-urls": true,
}

// Resolve reads settings from etcd's command line and environment. As in
// etcd, a configuration file named by --config-file or ETCD_CONFIG_FILE
// replaces every other flag and variable, and flags win over ETCD_*
// variables.
func Resolve(args, environ []string, readFile func(string) ([]byte, error)) (Settings, error) {
	values := make(map[string]string)
	for _, kv := range environ {
		key, value, _ := strings.Cut(kv, "=")
		for name := range valueFlags {
			if key == envName(name) {
				values[name] = value
			}
		}
	}
	for _, f := range flagscan.Scan(args, func(name string) bool { return valueFlags[name] }) {
		if valueFlags[f.Name] && f.HasValue {
			values[f.Name] = f.Value
		}
	}

	if path := values["config-file"]; path != "" {
		return fromFile(path, readFile)
	}
	return Settings{
		CertFile:            values["cert-file"],
		KeyFile:             values["key-file"],
		TrustedCAFile:       values["trusted-ca-file"],
		ListenClientURLs:    urls(values["listen-client-urls"]),
		AdvertiseClientURLs: urls(values["advertise-client-urls"]),
	}, nil
}

func fromFile(path string, readFile func(string) ([]byte, error)) (Settings, error) {
	data, err := readFile(path)
	if err != nil {
		return Settings{}, fmt.Errorf("read etcd config file: %w", err)
	}
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return Settings{}, fmt.Errorf("parse etcd config file %s: %w", path, err)
	}
	return Settings{
		CertFile:            fc.ClientSecurity.CertFile,
		KeyFile:             fc.ClientSecurity.KeyFile,
		TrustedCAFile:       fc.ClientSecurity.TrustedCAFile,
		ListenClientURLs:    urls(fc.ListenClientURLs),
		AdvertiseClientURLs: urls(fc.AdvertiseClientURLs),
	}, nil
}

func envName(flag string) string {
	return "ETCD_" + strings.ToUpper(strings.ReplaceAll(flag, "-", "_"))
}

func urls(list string) []string {
	if list == "" {
		list = defaultClientURL
	}
	parts := strings.Split(list, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// LocalClientURL returns the URL for reaching the local member through one
// of its client listeners. A loopback or wildcard listener is preferred,
// because the server certificates issued by Talos and etcd-operator cover
// loopback addresses.
func (s Settings) LocalClientURL() (string, error) {
	var fallback string
	for _, raw := range s.ListenClientURLs {
		u, err := url.Parse(raw)
		if err != nil {
			return "", fmt.Errorf("parse client URL: %w", err)
		}
		if u.Scheme == "unix" || u.Scheme == "unixs" {
			continue
		}
		host := u.Hostname()
		ip := net.ParseIP(host)
		switch {
		case ip != nil && ip.IsUnspecified():
			loopback := "127.0.0.1"
			if ip.To4() == nil {
				loopback = "::1"
			}
			u.Host = net.JoinHostPort(loopback, u.Port())
			return u.String(), nil
		case host == "localhost" || ip != nil && ip.IsLoopback():
			return u.String(), nil
		case fallback == "":
			fallback = u.String()
		}
	}
	if fallback == "" {
		return "", errors.New("etcd has no TCP client listener")
	}
	return fallback, nil
}
