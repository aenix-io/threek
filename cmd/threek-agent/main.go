// Command threek-agent runs the threek agent next to etcd: as a sidecar
// container, with the settings shared by the cluster in a mounted ConfigMap
// that is reloaded on every poll, or as a static Pod with every setting in
// flags.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aenix-io/threek/internal/agent"
	"github.com/aenix-io/threek/internal/config"
	"github.com/aenix-io/threek/internal/etcdconf"
)

const (
	// retryDelay paces restarts of an agent that could not start. The sidecar
	// never exits on such errors: a sidecar that is not running makes the
	// whole Pod, and so the etcd member, not ready.
	retryDelay = 10 * time.Second
	// remindAfter repeats an error that keeps the agent from starting, so it
	// stays visible in recent logs.
	remindAfter = 10 * time.Minute
)

type options struct {
	settings      etcdconf.Settings
	identity      string
	configDir     string
	metricsListen string
	// shared holds the shared settings given as flags, used without a
	// config directory.
	shared config.Config
}

func parseOptions(args []string, getenv func(string) string) (options, error) {
	var opts options
	fs := flag.NewFlagSet("threek-agent", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	endpoint := fs.String("endpoint", "https://127.0.0.1:2379", "client URL of the local etcd member")
	fs.StringVar(&opts.settings.CertFile, "cert-file", "", "client certificate for etcd")
	fs.StringVar(&opts.settings.KeyFile, "key-file", "", "client key for etcd")
	fs.StringVar(&opts.settings.TrustedCAFile, "trusted-ca-file", "", "CA that signed the etcd server certificate")
	fs.StringVar(&opts.settings.PeerCertFile, "peer-cert-file", "", "certificate for probing peer URLs")
	fs.StringVar(&opts.settings.PeerKeyFile, "peer-key-file", "", "key for probing peer URLs")
	fs.StringVar(&opts.settings.PeerTrustedCAFile, "peer-trusted-ca-file", "", "CA that signed the peer certificates")
	fs.StringVar(&opts.identity, "identity", getenv("THREEK_IDENTITY"), "label of this member, unique within the cluster")
	fs.StringVar(&opts.configDir, "config-dir", "", "directory of the mounted threek ConfigMap, reloaded on every poll")
	fs.StringVar(&opts.metricsListen, "metrics-listen", "", "address for the Prometheus endpoint; empty disables it")
	opts.shared = config.Default()
	var sharedFlags []string
	for _, name := range config.Shared() {
		fs.Func(name, "shared setting, used without --config-dir", func(value string) error {
			sharedFlags = append(sharedFlags, name)
			return opts.shared.Set(name, value)
		})
	}
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	switch {
	case fs.NArg() > 0:
		return options{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	case opts.configDir != "" && len(sharedFlags) > 0:
		return options{}, fmt.Errorf("--%s cannot be combined with --config-dir", sharedFlags[0])
	case opts.shared.PreferredLeader != "" && opts.identity == "":
		return options{}, errors.New("--preferred-leader requires --identity")
	}
	opts.settings.ListenClientURLs = []string{*endpoint}
	return opts, nil
}

func (o options) load() (config.Config, error) {
	cfg := o.shared
	if o.configDir != "" {
		var err error
		if cfg, err = config.FromDir(os.DirFS(o.configDir)); err != nil {
			return config.Config{}, err
		}
	}
	if cfg.PreferredLeader != "" && o.identity == "" {
		return config.Config{}, errors.New("preferred-leader requires --identity")
	}
	cfg.Identity = o.identity
	cfg.MetricsListen = o.metricsListen
	return cfg, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("logger", "threek", "component", "agent")
	opts, err := parseOptions(os.Args[1:], os.Getenv)
	if err != nil {
		logger.Error("invalid arguments", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// The agent starts off and takes its mode from the first reload.
	initial := config.Default()
	initial.Identity = opts.identity
	initial.MetricsListen = opts.metricsListen
	keepRunning(ctx, func(ctx context.Context) error {
		return agent.Run(ctx, initial, opts.load, opts.settings, logger)
	}, logger, retryDelay, remindAfter)
}

// keepRunning restarts run until ctx is done. An error that repeats, such as
// a missing certificate, is logged again only after remind.
func keepRunning(ctx context.Context, run func(context.Context) error, logger *slog.Logger, delay, remind time.Duration) {
	var last string
	var loggedAt time.Time
	for {
		err := run(ctx)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err == nil:
			last = ""
			logger.Error("agent stopped unexpectedly, restarting", "after", delay.String())
		case err.Error() != last || time.Since(loggedAt) >= remind:
			last, loggedAt = err.Error(), time.Now()
			logger.Error("agent stopped, restarting", "error", err, "after", delay.String())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
