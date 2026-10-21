package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aenix-io/threek/internal/config"
	"github.com/aenix-io/threek/internal/etcdconf"
)

func TestParseOptions(t *testing.T) {
	opts, err := parseOptions([]string{
		"--endpoint=https://127.0.0.1:2379",
		"--cert-file=/tls/client/tls.crt",
		"--key-file=/tls/client/tls.key",
		"--trusted-ca-file=/tls/client/ca.crt",
		"--peer-cert-file=/tls/peer/tls.crt",
		"--peer-key-file=/tls/peer/tls.key",
		"--peer-trusted-ca-file=/tls/peer/ca.crt",
		"--config-dir=/etc/threek",
		"--metrics-listen=:2390",
	}, func(name string) string { return map[string]string{"THREEK_IDENTITY": "zone-a"}[name] })
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	want := etcdconf.Settings{
		CertFile:          "/tls/client/tls.crt",
		KeyFile:           "/tls/client/tls.key",
		TrustedCAFile:     "/tls/client/ca.crt",
		ListenClientURLs:  []string{"https://127.0.0.1:2379"},
		PeerCertFile:      "/tls/peer/tls.crt",
		PeerKeyFile:       "/tls/peer/tls.key",
		PeerTrustedCAFile: "/tls/peer/ca.crt",
	}
	if !reflect.DeepEqual(opts.settings, want) {
		t.Errorf("settings = %+v, want %+v", opts.settings, want)
	}
	if opts.identity != "zone-a" || opts.configDir != "/etc/threek" || opts.metricsListen != ":2390" {
		t.Errorf("options = %+v", opts)
	}
}

func TestParseOptionsDefaults(t *testing.T) {
	opts, err := parseOptions(nil, func(string) string { return "" })
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if got := opts.settings.ListenClientURLs; len(got) != 1 || got[0] != "https://127.0.0.1:2379" {
		t.Errorf("endpoint = %q, want https://127.0.0.1:2379", got)
	}
	if opts.configDir != "/etc/threek" {
		t.Errorf("config dir = %q, want /etc/threek", opts.configDir)
	}
}

func TestIdentityFlagWinsOverEnvironment(t *testing.T) {
	opts, err := parseOptions([]string{"--identity=node-1"}, func(string) string { return "zone-a" })
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if opts.identity != "node-1" {
		t.Errorf("identity = %q, want the flag's node-1", opts.identity)
	}
}

func TestParseOptionsErrors(t *testing.T) {
	for _, args := range [][]string{{"--threek-mode=enforce"}, {"stray"}} {
		if _, err := parseOptions(args, func(string) string { return "" }); err == nil {
			t.Errorf("parseOptions(%q) error = nil", args)
		}
	}
}

func TestLoadAddsPerMemberSettings(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "mode", "enforce")
	write(t, dir, "preferred-leader", "zone-c")
	opts := options{configDir: dir, identity: "zone-a", metricsListen: ":2390"}
	cfg, err := opts.load()
	if err != nil {
		t.Fatalf("load() error = %v", err)
	}
	if cfg.Mode != config.ModeEnforce || cfg.PreferredLeader != "zone-c" || cfg.Identity != "zone-a" || cfg.MetricsListen != ":2390" {
		t.Errorf("config = %+v", cfg)
	}
}

func TestLoadPreferredLeaderNeedsIdentity(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "preferred-leader", "zone-c")
	_, err := options{configDir: dir}.load()
	if err == nil || !strings.Contains(err.Error(), "preferred-leader requires --identity") {
		t.Errorf("load() error = %v, want the identity requirement", err)
	}
}

func TestLoadMissingDirectory(t *testing.T) {
	_, err := options{configDir: filepath.Join(t.TempDir(), "absent")}.load()
	if err == nil {
		t.Error("load() error = nil for a missing directory")
	}
}

func write(t *testing.T, dir, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o644); err != nil {
		t.Fatal(err)
	}
}
