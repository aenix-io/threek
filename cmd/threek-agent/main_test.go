package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

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
	}, func(name string) string { return map[string]string{"THREEK_IDENTITY": "zone-a"}[name] }, io.Discard)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	want := etcdconf.Settings{
		CertFile:          "/tls/client/tls.crt",
		KeyFile:           "/tls/client/tls.key",
		TrustedCAFile:     "/tls/client/ca.crt",
		Endpoint:          "https://127.0.0.1:2379",
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
	opts, err := parseOptions(nil, func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if got := opts.settings.Endpoint; got != "https://127.0.0.1:2379" {
		t.Errorf("endpoint = %q, want https://127.0.0.1:2379", got)
	}
	if opts.configDir != "" {
		t.Errorf("config dir = %q, want none", opts.configDir)
	}
	cfg, err := opts.load()
	if err != nil || cfg.Mode != config.ModeOff {
		t.Errorf("load() = %+v, %v; want threek off", cfg, err)
	}
}

func TestIdentityFlagWinsOverEnvironment(t *testing.T) {
	opts, err := parseOptions([]string{"--identity=node-1"}, func(string) string { return "zone-a" }, io.Discard)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if opts.identity != "node-1" {
		t.Errorf("identity = %q, want the flag's node-1", opts.identity)
	}
}

func TestParseOptionsErrors(t *testing.T) {
	for _, args := range [][]string{{"--bogus"}, {"--mode="}, {"stray"}} {
		if _, err := parseOptions(args, func(string) string { return "" }, io.Discard); err == nil {
			t.Errorf("parseOptions(%q) error = nil", args)
		}
	}
}

func TestSettingsFromFlags(t *testing.T) {
	opts, err := parseOptions([]string{"--identity=zone-a", "--mode=enforce", "--preferred-leader=zone-c", "--move-after=20s"}, func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	cfg, err := opts.load()
	if err != nil {
		t.Fatalf("load() error = %v", err)
	}
	if cfg.Mode != config.ModeEnforce || cfg.PreferredLeader != "zone-c" || cfg.MoveAfter != 20*time.Second || cfg.Identity != "zone-a" {
		t.Errorf("config = %+v", cfg)
	}
}

func TestSettingsFromFlagsErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--mode=on"},
		{"--config-dir=/etc/threek", "--mode=enforce"},
		{"--preferred-leader=zone-c"},
	} {
		if _, err := parseOptions(args, func(string) string { return "" }, io.Discard); err == nil {
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

func TestKeepRunningLogsEachErrorOnce(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	errs := []error{errors.New("no CA"), errors.New("no CA"), errors.New("no CA"), errors.New("refused"), nil}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	keepRunning(ctx, func(context.Context) error {
		err := errs[calls]
		calls++
		if calls == len(errs) {
			cancel()
		}
		return err
	}, logger, time.Millisecond, time.Hour)
	if calls != len(errs) {
		t.Fatalf("run called %d times, want %d", calls, len(errs))
	}
	if n := strings.Count(logs.String(), "no CA"); n != 1 {
		t.Errorf("a repeated error logged %d times, want once:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "refused") {
		t.Errorf("a new error is not logged:\n%s", logs.String())
	}
}

func TestKeepRunningRepeatsAPersistentError(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	keepRunning(ctx, func(context.Context) error {
		calls++
		if calls == 3 {
			cancel()
		}
		return errors.New("no CA")
	}, logger, time.Millisecond, 0)
	if n := strings.Count(logs.String(), "no CA"); n != 2 {
		t.Errorf("a persistent error logged %d times, want once per reminder:\n%s", n, logs.String())
	}
}

func TestKeepRunningLogsAnUnexpectedStop(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	keepRunning(ctx, func(context.Context) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return nil
	}, logger, time.Millisecond, time.Hour)
	if !strings.Contains(logs.String(), "agent stopped unexpectedly") {
		t.Errorf("a stop without an error is not logged:\n%s", logs.String())
	}
}

// A successful start ends the previous error, so the same error coming back
// later is news.
func TestKeepRunningLogsAnErrorAgainAfterAStart(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	errs := []error{errors.New("no CA"), nil, errors.New("no CA"), nil}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	keepRunning(ctx, func(context.Context) error {
		err := errs[calls]
		calls++
		if calls == len(errs) {
			cancel()
		}
		return err
	}, logger, time.Millisecond, time.Hour)
	if n := strings.Count(logs.String(), "no CA"); n != 2 {
		t.Errorf("an error that came back logged %d times, want 2:\n%s", n, logs.String())
	}
}

func TestHelpListsTheFlags(t *testing.T) {
	var out bytes.Buffer
	_, err := parseOptions([]string{"--help"}, func(string) string { return "" }, &out)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseOptions(--help) error = %v, want flag.ErrHelp", err)
	}
	for _, name := range []string{"-endpoint", "-mode", "-config-dir", "-peer-trusted-ca-file"} {
		if !strings.Contains(out.String(), name) {
			t.Errorf("usage lacks %s:\n%s", name, out.String())
		}
	}
}

func TestWaitForReturnsOnceEveryFileExists(t *testing.T) {
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "etcd-client.crt"), filepath.Join(dir, "etcd-client.key")}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	done := make(chan error, 1)
	go func() { done <- waitFor(context.Background(), paths, time.Millisecond, logger) }()

	time.Sleep(20 * time.Millisecond)
	write(t, dir, "etcd-client.crt", "x")
	select {
	case err := <-done:
		t.Fatalf("waitFor() returned %v with a file still missing", err)
	case <-time.After(20 * time.Millisecond):
	}
	write(t, dir, "etcd-client.key", "x")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitFor() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitFor() did not return once every file existed")
	}
	if !strings.Contains(logs.String(), "waiting for files") {
		t.Errorf("the wait is not logged:\n%s", logs.String())
	}
}

func TestWaitForDoesNotTakeADirectoryForAFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "etcd-client.crt")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := waitFor(ctx, []string{path}, time.Millisecond, slog.New(slog.NewJSONHandler(io.Discard, nil))); err == nil {
		t.Error("waitFor() error = nil for a directory in place of the file")
	}
}

func TestWaitForFlag(t *testing.T) {
	opts, err := parseOptions([]string{"--wait-for=/a", "--wait-for=/b"}, func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if !slices.Equal(opts.waitFor, []string{"/a", "/b"}) {
		t.Errorf("wait for = %v", opts.waitFor)
	}
}
