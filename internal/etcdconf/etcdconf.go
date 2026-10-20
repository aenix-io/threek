// Package etcdconf resolves the etcd settings the agent needs, following
// etcd's own precedence rules.
package etcdconf

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
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
	PeerCertFile        string
	PeerKeyFile         string
	PeerTrustedCAFile   string
	PeerAutoTLS         bool
}

// fileConfig mirrors the keys etcd reads from --config-file.
type fileConfig struct {
	ListenClientURLs    string         `json:"listen-client-urls"`
	AdvertiseClientURLs string         `json:"advertise-client-urls"`
	ClientSecurity      securityConfig `json:"client-transport-security"`
	PeerSecurity        securityConfig `json:"peer-transport-security"`
}

type securityConfig struct {
	CertFile      string `json:"cert-file"`
	KeyFile       string `json:"key-file"`
	TrustedCAFile string `json:"trusted-ca-file"`
	AutoTLS       bool   `json:"auto-tls"`
}

var valueFlags = map[string]bool{
	"config-file":           true,
	"cert-file":             true,
	"key-file":              true,
	"trusted-ca-file":       true,
	"listen-client-urls":    true,
	"advertise-client-urls": true,
	"peer-cert-file":        true,
	"peer-key-file":         true,
	"peer-trusted-ca-file":  true,
}

const peerAutoTLS = "peer-auto-tls"

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
		if key == envName(peerAutoTLS) {
			values[peerAutoTLS] = value
		}
	}
	for _, f := range flagscan.Scan(args, func(name string) bool { return valueFlags[name] }) {
		switch {
		case valueFlags[f.Name] && f.HasValue:
			values[f.Name] = f.Value
		case f.Name == peerAutoTLS && !f.HasValue:
			values[peerAutoTLS] = "true"
		case f.Name == peerAutoTLS:
			values[peerAutoTLS] = f.Value
		}
	}

	if path := values["config-file"]; path != "" {
		return fromFile(path, readFile)
	}
	autoTLS := false
	if v := values[peerAutoTLS]; v != "" {
		var err error
		if autoTLS, err = strconv.ParseBool(v); err != nil {
			return Settings{}, fmt.Errorf("invalid %s value %q", peerAutoTLS, v)
		}
	}
	return Settings{
		CertFile:            values["cert-file"],
		KeyFile:             values["key-file"],
		TrustedCAFile:       values["trusted-ca-file"],
		ListenClientURLs:    urls(values["listen-client-urls"]),
		AdvertiseClientURLs: urls(values["advertise-client-urls"]),
		PeerCertFile:        values["peer-cert-file"],
		PeerKeyFile:         values["peer-key-file"],
		PeerTrustedCAFile:   values["peer-trusted-ca-file"],
		PeerAutoTLS:         autoTLS,
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
		PeerCertFile:        fc.PeerSecurity.CertFile,
		PeerKeyFile:         fc.PeerSecurity.KeyFile,
		PeerTrustedCAFile:   fc.PeerSecurity.TrustedCAFile,
		PeerAutoTLS:         fc.PeerSecurity.AutoTLS,
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
