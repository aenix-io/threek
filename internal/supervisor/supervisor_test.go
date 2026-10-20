package supervisor

import (
	"bufio"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const helperEnv = "SUPERVISOR_TEST_HELPER"

// TestMain lets the test binary act as etcd or agent child processes.
func TestMain(m *testing.M) {
	if behavior := os.Getenv(helperEnv); behavior != "" {
		os.Exit(helper(behavior))
	}
	os.Exit(m.Run())
}

func helper(behavior string) int {
	name, arg, _ := strings.Cut(behavior, ":")
	switch name {
	case "exit":
		code, _ := strconv.Atoi(arg)
		return code
	case "trap":
		// Exit with the given code on SIGTERM, after reporting readiness on stdout.
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGTERM)
		_, _ = os.Stdout.WriteString("ready\n")
		<-sigs
		code, _ := strconv.Atoi(arg)
		return code
	case "kill-self":
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	case "stopped-marker":
		// Create arg+".ready", wait for SIGTERM, then create the file named by arg.
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGTERM)
		_ = os.WriteFile(arg+".ready", nil, 0o600)
		<-sigs
		_ = os.WriteFile(arg, nil, 0o600)
		return 0
	case "ignore-term":
		signal.Ignore(syscall.SIGTERM)
		_ = os.WriteFile(arg, nil, 0o600)
		select {}
	}
	return 99
}

func helperCmd(t *testing.T, behavior string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), helperEnv+"="+behavior)
	cmd.Stderr = os.Stderr
	return cmd
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("file %s did not appear", path)
}

// waitFileQuietly is waitFile for goroutines other than the test's own,
// which must not call t.Fatal.
func waitFileQuietly(path string) {
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func run(t *testing.T, s *Supervisor) int {
	t.Helper()
	type result struct {
		code int
		err  error
	}
	done := make(chan result, 1)
	go func() {
		state, err := s.Run()
		if err != nil {
			done <- result{err: err}
			return
		}
		done <- result{code: ExitCode(state)}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Run() error = %v", r.err)
		}
		return r.code
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return")
		return 0
	}
}

func idleAgent(t *testing.T) func() *exec.Cmd {
	return func() *exec.Cmd { return helperCmd(t, "trap:0") }
}

func TestRunReturnsEtcdExitCode(t *testing.T) {
	s := &Supervisor{Etcd: helperCmd(t, "exit:3"), NewAgent: idleAgent(t), Logger: testLogger()}
	if code := run(t, s); code != 3 {
		t.Errorf("exit code = %d, want 3", code)
	}
}

func TestRunReportsEtcdKilledBySignal(t *testing.T) {
	s := &Supervisor{Etcd: helperCmd(t, "kill-self"), NewAgent: idleAgent(t), Logger: testLogger()}
	if code := run(t, s); code != 128+int(syscall.SIGKILL) {
		t.Errorf("exit code = %d, want %d", code, 128+int(syscall.SIGKILL))
	}
}

func TestRunForwardsSignalsToEtcd(t *testing.T) {
	etcd := helperCmd(t, "trap:7")
	stdout, err := etcd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	signals := make(chan os.Signal, 1)
	s := &Supervisor{Etcd: etcd, NewAgent: idleAgent(t), Signals: signals, Logger: testLogger()}
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		if line == "ready\n" {
			signals <- syscall.SIGTERM
		}
	}()
	if code := run(t, s); code != 7 {
		t.Errorf("exit code = %d, want 7", code)
	}
}

func TestRunStopsAgentWhenEtcdExits(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "agent-stopped")
	etcd := helperCmd(t, "trap:0")
	stdout, err := etcd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	signals := make(chan os.Signal, 1)
	s := &Supervisor{
		Etcd:     etcd,
		NewAgent: func() *exec.Cmd { return helperCmd(t, "stopped-marker:"+marker) },
		Signals:  signals,
		Logger:   testLogger(),
	}
	go func() {
		_, _ = bufio.NewReader(stdout).ReadString('\n')
		waitFileQuietly(marker + ".ready")
		signals <- syscall.SIGTERM
	}()
	run(t, s)
	waitFile(t, marker)
}

func TestRunKillsAgentThatIgnoresTermination(t *testing.T) {
	started := filepath.Join(t.TempDir(), "agent-started")
	etcd := helperCmd(t, "trap:0")
	stdout, err := etcd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	signals := make(chan os.Signal, 1)
	s := &Supervisor{
		Etcd:             etcd,
		NewAgent:         func() *exec.Cmd { return helperCmd(t, "ignore-term:"+started) },
		Signals:          signals,
		Logger:           testLogger(),
		AgentStopTimeout: 100 * time.Millisecond,
	}
	go func() {
		_, _ = bufio.NewReader(stdout).ReadString('\n')
		waitFileQuietly(started)
		signals <- syscall.SIGTERM
	}()
	if code := run(t, s); code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
}

func TestRunRestartsCrashingAgent(t *testing.T) {
	etcd := helperCmd(t, "trap:0")
	stdout, err := etcd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	signals := make(chan os.Signal, 1)
	var starts atomic.Int32
	s := &Supervisor{
		Etcd: etcd,
		NewAgent: func() *exec.Cmd {
			if starts.Add(1) == 3 {
				signals <- syscall.SIGTERM
			}
			return helperCmd(t, "exit:2")
		},
		Signals:      signals,
		Logger:       testLogger(),
		AgentBackoff: Backoff{Initial: 10 * time.Millisecond, Max: 20 * time.Millisecond},
	}
	go func() { _, _ = io.Copy(io.Discard, stdout) }()
	if code := run(t, s); code != 0 {
		t.Errorf("exit code = %d, want 0: etcd must survive agent crashes", code)
	}
	if n := starts.Load(); n < 3 {
		t.Errorf("agent started %d times, want at least 3", n)
	}
}

func TestRunFailsWhenEtcdCannotStart(t *testing.T) {
	s := &Supervisor{
		Etcd:     exec.Command(filepath.Join(t.TempDir(), "missing")),
		NewAgent: idleAgent(t),
		Logger:   testLogger(),
	}
	if _, err := s.Run(); err == nil {
		t.Error("Run() error = nil, want an error")
	}
}

func TestBackoff(t *testing.T) {
	b := Backoff{Initial: time.Second, Max: 4 * time.Second}
	var got []time.Duration
	d := time.Duration(0)
	for range 5 {
		d = b.Next(d)
		got = append(got, d)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second, 4 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Next sequence = %v, want %v", got, want)
		}
	}
}

func TestIsServerInvocation(t *testing.T) {
	tests := []struct {
		args []string
		want bool
	}{
		{args: nil, want: true},
		{args: []string{"--name=a", "--data-dir", "/var/lib/etcd"}, want: true},
		{args: []string{"--version"}, want: false},
		{args: []string{"-version"}, want: false},
		{args: []string{"--version=true"}, want: false},
		{args: []string{"--version=false"}, want: true},
		{args: []string{"--help"}, want: false},
		{args: []string{"-h"}, want: false},
		{args: []string{"gateway", "start"}, want: false},
		{args: []string{"grpc-proxy", "start"}, want: false},
		{args: []string{"--name", "version"}, want: true},
	}
	for _, tt := range tests {
		if got := IsServerInvocation(tt.args); got != tt.want {
			t.Errorf("IsServerInvocation(%q) = %v, want %v", tt.args, got, tt.want)
		}
	}
}
