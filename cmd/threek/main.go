// Command threek replaces the etcd binary: it starts upstream etcd with the
// original arguments and runs the threek agent next to it.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aenix-io/threek/internal/agent"
	"github.com/aenix-io/threek/internal/config"
	"github.com/aenix-io/threek/internal/etcdconf"
	"github.com/aenix-io/threek/internal/supervisor"
)

const (
	// agentName is argv[0] of the agent process, which is this same binary.
	agentName = "threek-agent"
	// upstreamName is the upstream etcd binary, next to this one.
	upstreamName = "etcd-upstream"
)

func main() {
	os.Exit(run(os.Args, os.Environ()))
}

func run(argv, environ []string) int {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("logger", "threek")

	cfg, etcdArgs, etcdEnv, err := config.Parse(argv[1:], environ)
	if err != nil {
		logger.Error("invalid threek configuration", "error", err)
		return 2
	}
	if filepath.Base(argv[0]) == agentName {
		return runAgent(cfg, etcdArgs, etcdEnv, logger.With("component", "agent"))
	}
	logger = logger.With("component", "supervisor")

	self, err := os.Executable()
	if err != nil {
		logger.Error("locate own executable", "error", err)
		return 1
	}
	upstream := filepath.Join(filepath.Dir(self), upstreamName)
	etcdArgv := append([]string{argv[0]}, etcdArgs...)

	if cfg.Mode == config.ModeOff || !supervisor.IsServerInvocation(etcdArgs) {
		err := syscall.Exec(upstream, etcdArgv, etcdEnv) //nolint:gosec // upstream is next to this binary, not user input
		logger.Error("exec etcd", "path", upstream, "error", err)
		return 1
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
	s := supervisor.Supervisor{
		Etcd: &exec.Cmd{
			Path:   upstream,
			Args:   etcdArgv,
			Env:    etcdEnv,
			Stdin:  os.Stdin,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		},
		NewAgent: func() *exec.Cmd {
			return &exec.Cmd{
				Path:   self,
				Args:   append([]string{agentName}, argv[1:]...),
				Env:    environ,
				Stdout: os.Stderr,
				Stderr: os.Stderr,
			}
		},
		Signals: signals,
		Logger:  logger,
	}
	state, err := s.Run()
	if err != nil {
		logger.Error("run etcd", "error", err)
		return 1
	}
	return endLike(state, os.Getpid())
}

// endLike ends this process the way etcd ended. After a graceful shutdown on
// SIGINT or SIGTERM etcd kills itself with the same signal, or exits 0 when it
// is PID 1, because the kernel ignores such a signal sent to PID 1. With the
// supervisor as PID 1, etcd is no longer PID 1, so the supervisor takes over
// that rule.
func endLike(state *os.ProcessState, pid int) int {
	ws, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() {
		return state.ExitCode()
	}
	sig := ws.Signal()
	if pid == 1 {
		if sig == syscall.SIGINT || sig == syscall.SIGTERM {
			return 0
		}
		return supervisor.ExitCode(state)
	}
	signal.Reset(sig)
	_ = syscall.Kill(pid, sig)
	// Delivery is asynchronous; give it time before falling back to an exit code.
	time.Sleep(time.Second)
	return supervisor.ExitCode(state)
}

func runAgent(cfg config.Config, etcdArgs, etcdEnv []string, logger *slog.Logger) int {
	settings, err := etcdconf.Resolve(etcdArgs, etcdEnv, os.ReadFile)
	if err != nil {
		logger.Error("read etcd configuration", "error", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := agent.Run(ctx, cfg, nil, settings, logger); err != nil {
		logger.Error("agent stopped", "error", err)
		return 1
	}
	return 0
}
