// Package supervisor runs etcd next to the threek agent.
package supervisor

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"github.com/aenix-io/threek/internal/flagscan"
)

// Backoff is an exponential delay between agent restarts.
type Backoff struct {
	Initial time.Duration
	Max     time.Duration
}

// Next returns the delay that follows prev; a zero prev starts over.
func (b Backoff) Next(prev time.Duration) time.Duration {
	if prev <= 0 {
		return b.Initial
	}
	return min(prev*2, b.Max)
}

// Supervisor keeps etcd running unchanged and the agent running next to it.
// etcd's lifetime is the supervisor's lifetime; the agent is restarted
// whenever it exits and never affects etcd.
type Supervisor struct {
	// Etcd is the prepared, not yet started, etcd command.
	Etcd *exec.Cmd
	// NewAgent returns a new, not yet started, agent command.
	NewAgent func() *exec.Cmd
	// Signals are forwarded to etcd.
	Signals          <-chan os.Signal
	Logger           *slog.Logger
	AgentBackoff     Backoff
	AgentStopTimeout time.Duration
}

// Run starts etcd and the agent and returns etcd's exit code once etcd exits.
// A process killed by signal N is reported as 128+N, as shells do.
func (s *Supervisor) Run() (int, error) {
	if s.AgentBackoff == (Backoff{}) {
		s.AgentBackoff = Backoff{Initial: time.Second, Max: time.Minute}
	}
	if s.AgentStopTimeout == 0 {
		s.AgentStopTimeout = 5 * time.Second
	}

	ownProcessGroup(s.Etcd)
	if err := s.Etcd.Start(); err != nil {
		return 0, fmt.Errorf("start etcd: %w", err)
	}
	etcdDone := make(chan error, 1)
	go func() { etcdDone <- s.Etcd.Wait() }()

	var (
		agent      *exec.Cmd
		agentDone  chan error
		agentStart time.Time
		delay      time.Duration
		restart    <-chan time.Time
	)
	scheduleRestart := func() {
		if time.Since(agentStart) >= s.AgentBackoff.Max {
			delay = 0
		}
		delay = s.AgentBackoff.Next(delay)
		s.Logger.Info("restarting agent", "delay", delay.String())
		restart = time.After(delay)
	}
	startAgent := func() {
		restart = nil
		agent = s.NewAgent()
		ownProcessGroup(agent)
		agentStart = time.Now()
		if err := agent.Start(); err != nil {
			s.Logger.Error("start agent", "error", err)
			agent = nil
			scheduleRestart()
			return
		}
		agentDone = make(chan error, 1)
		go func(cmd *exec.Cmd, done chan<- error) { done <- cmd.Wait() }(agent, agentDone)
	}
	startAgent()

	for {
		select {
		case sig := <-s.Signals:
			if err := s.Etcd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
				s.Logger.Error("forward signal to etcd", "signal", sig.String(), "error", err)
			}
		case err := <-etcdDone:
			if _, exited := errors.AsType[*exec.ExitError](err); err != nil && !exited {
				s.Logger.Error("wait for etcd", "error", err)
			}
			s.stopAgent(agent, agentDone)
			return exitCode(s.Etcd.ProcessState), nil
		case err := <-agentDone:
			s.Logger.Error("agent exited", "error", err)
			agent, agentDone = nil, nil
			scheduleRestart()
		case <-restart:
			startAgent()
		}
	}
}

func (s *Supervisor) stopAgent(agent *exec.Cmd, done <-chan error) {
	if agent == nil {
		return
	}
	_ = agent.Process.Signal(syscall.SIGTERM)
	var err error
	select {
	case err = <-done:
	case <-time.After(s.AgentStopTimeout):
		s.Logger.Warn("agent did not stop in time, killing it")
		_ = agent.Process.Kill()
		err = <-done
	}
	s.Logger.Info("agent stopped", "error", err)
}

// ownProcessGroup keeps a terminal's Ctrl-C from reaching the child directly:
// the supervisor forwards it, and a second SIGINT would cut etcd's graceful
// shutdown short.
func ownProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

func exitCode(state *os.ProcessState) int {
	if ws, ok := state.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return state.ExitCode()
}

// IsServerInvocation reports whether etcd's arguments start a server, as
// opposed to printing the version or help, or running a subcommand such as
// gateway or grpc-proxy.
func IsServerInvocation(args []string) bool {
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		return false
	}
	for _, f := range flagscan.Scan(args, func(string) bool { return false }) {
		switch f.Name {
		case "version", "help", "h":
			if !f.HasValue {
				return false
			}
			if on, err := strconv.ParseBool(f.Value); err != nil || on {
				return false
			}
		}
	}
	return true
}
