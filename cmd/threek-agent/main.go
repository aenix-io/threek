// Command threek-agent runs the threek agent as a sidecar container next to
// etcd. Settings shared by the cluster come from a mounted ConfigMap and are
// reloaded on every poll.
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

// retryDelay paces restarts of an agent that could not start. The sidecar
// never exits on such errors: a sidecar that is not running makes the whole
// Pod, and so the etcd member, not ready.
const retryDelay = 10 * time.Second

type options struct {
	settings      etcdconf.Settings
	identity      string
	configDir     string
	metricsListen string
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
	fs.StringVar(&opts.configDir, "config-dir", "/etc/threek", "directory of the mounted threek ConfigMap")
	fs.StringVar(&opts.metricsListen, "metrics-listen", "", "address for the Prometheus endpoint; empty disables it")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	opts.settings.ListenClientURLs = []string{*endpoint}
	return opts, nil
}

func (o options) load() (config.Config, error) {
	cfg, err := config.FromDir(os.DirFS(o.configDir))
	if err != nil {
		return config.Config{}, err
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
	for {
		err := agent.Run(ctx, initial, opts.load, opts.settings, logger)
		if ctx.Err() != nil {
			return
		}
		logger.Error("agent stopped, restarting", "error", err, "after", retryDelay.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(retryDelay):
		}
	}
}
