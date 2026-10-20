package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// fakeEtcd records what it was started with and exits with $FAKE_EXIT.
const fakeEtcd = `#!/bin/sh
printf '%s\n' "$@" > "$FAKE_OUT/args"
env > "$FAKE_OUT/env"
echo "$$ $PPID" > "$FAKE_OUT/pids"
echo etcd-stdout
[ -n "$FAKE_SIGNAL" ] && kill "-$FAKE_SIGNAL" $$
exit "${FAKE_EXIT:-0}"
`

var binDir string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "threek-main-test")
	if err != nil {
		panic(err)
	}
	binDir = dir
	build := exec.Command("go", "build", "-o", filepath.Join(dir, "etcd"), ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "etcd-upstream"), []byte(fakeEtcd), 0o755); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	code    int
	signal  syscall.Signal
	pid     int
	stdout  string
	stderr  string
	started bool
	args    []string
	env     []string
	// fakePID and fakeParent are the fake etcd's own and parent process IDs.
	fakePID, fakeParent int
}

func runThreek(t *testing.T, env []string, args ...string) result {
	t.Helper()
	out := t.TempDir()
	cmd := exec.Command(filepath.Join(binDir, "etcd"), args...)
	cmd.Env = append([]string{"PATH=/usr/bin:/bin", "FAKE_OUT=" + out}, env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	r := result{stdout: stdout.String(), stderr: stderr.String(), pid: cmd.Process.Pid}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		r.code = exitErr.ExitCode()
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			r.signal = ws.Signal()
		}
	} else if err != nil {
		t.Fatalf("run threek: %v", err)
	}
	pids, err := os.ReadFile(filepath.Join(out, "pids"))
	if err != nil {
		return r
	}
	r.started = true
	fields := strings.Fields(string(pids))
	r.fakePID, _ = strconv.Atoi(fields[0])
	r.fakeParent, _ = strconv.Atoi(fields[1])
	argData, _ := os.ReadFile(filepath.Join(out, "args"))
	r.args = strings.Fields(string(argData))
	envData, _ := os.ReadFile(filepath.Join(out, "env"))
	r.env = strings.Split(strings.TrimSpace(string(envData)), "\n")
	return r
}

func assertNoThreekSettings(t *testing.T, r result) {
	t.Helper()
	for _, a := range r.args {
		if strings.Contains(a, "threek") {
			t.Errorf("etcd got threek flag %q", a)
		}
	}
	for _, kv := range r.env {
		if strings.HasPrefix(kv, "THREEK_") {
			t.Errorf("etcd got threek variable %q", kv)
		}
	}
}

func TestModeOffExecsEtcd(t *testing.T) {
	r := runThreek(t, []string{"FAKE_EXIT=5", "THREEK_IDENTITY=zone-a"}, "--threek-mode=off", "--name=a")
	if !r.started {
		t.Fatalf("etcd was not started, stderr: %s", r.stderr)
	}
	if r.fakePID != r.pid {
		t.Errorf("etcd pid = %d, want threek's own pid %d: mode off must exec etcd", r.fakePID, r.pid)
	}
	if r.code != 5 {
		t.Errorf("exit code = %d, want 5", r.code)
	}
	if !slices.Equal(r.args, []string{"--name=a"}) {
		t.Errorf("etcd args = %q, want [--name=a]", r.args)
	}
	assertNoThreekSettings(t, r)
}

func TestVersionExecsEtcd(t *testing.T) {
	r := runThreek(t, nil, "--threek-mode=enforce", "--version")
	if !r.started {
		t.Fatalf("etcd was not started, stderr: %s", r.stderr)
	}
	if r.fakePID != r.pid {
		t.Errorf("etcd pid = %d, want threek's own pid %d: --version must exec etcd", r.fakePID, r.pid)
	}
	if !slices.Equal(r.args, []string{"--version"}) {
		t.Errorf("etcd args = %q, want [--version]", r.args)
	}
}

func TestServerRunsUnderSupervisor(t *testing.T) {
	r := runThreek(t,
		[]string{"FAKE_EXIT=7", "THREEK_IDENTITY=zone-a"},
		"--threek-mode", "dry-run", "--name=a", "--data-dir", "/var/lib/etcd",
	)
	if !r.started {
		t.Fatalf("etcd was not started, stderr: %s", r.stderr)
	}
	if r.fakeParent != r.pid {
		t.Errorf("etcd parent pid = %d, want threek's pid %d", r.fakeParent, r.pid)
	}
	if r.code != 7 {
		t.Errorf("exit code = %d, want 7", r.code)
	}
	if want := []string{"--name=a", "--data-dir", "/var/lib/etcd"}; !slices.Equal(r.args, want) {
		t.Errorf("etcd args = %q, want %q", r.args, want)
	}
	assertNoThreekSettings(t, r)
	if r.stdout != "etcd-stdout\n" {
		t.Errorf("stdout = %q, want only etcd's output", r.stdout)
	}
}

// etcd re-raises SIGTERM on itself after a graceful shutdown; the supervisor
// must end the same way so that whoever started it sees no difference.
func TestEtcdKilledBySignalKillsSupervisor(t *testing.T) {
	r := runThreek(t, []string{"FAKE_SIGNAL=TERM"}, "--threek-mode=dry-run", "--name=a")
	if !r.started {
		t.Fatalf("etcd was not started, stderr: %s", r.stderr)
	}
	if r.fakeParent != r.pid {
		t.Fatalf("etcd parent pid = %d, want threek's pid %d", r.fakeParent, r.pid)
	}
	if r.signal != syscall.SIGTERM {
		t.Errorf("threek ended with code %d, signal %v; want killed by SIGTERM", r.code, r.signal)
	}
}

func TestInvalidConfigurationDoesNotStartEtcd(t *testing.T) {
	r := runThreek(t, nil, "--threek-mode=on", "--name=a")
	if r.started {
		t.Error("etcd was started")
	}
	if r.code != 2 {
		t.Errorf("exit code = %d, want 2", r.code)
	}
	if !strings.Contains(r.stderr, "--threek-mode") {
		t.Errorf("stderr = %q, want it to name the bad flag", r.stderr)
	}
}

func processState(t *testing.T, script string) *os.ProcessState {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", script)
	_ = cmd.Run()
	return cmd.ProcessState
}

func TestEndLikeAsPID1(t *testing.T) {
	tests := []struct {
		name   string
		script string
		want   int
	}{
		{name: "exit code", script: "exit 3", want: 3},
		{name: "SIGTERM", script: "kill -TERM $$", want: 0},
		{name: "SIGINT", script: "kill -INT $$", want: 0},
		{name: "SIGKILL", script: "kill -KILL $$", want: 128 + int(syscall.SIGKILL)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := endLike(processState(t, tt.script), 1); got != tt.want {
				t.Errorf("endLike() = %d, want %d", got, tt.want)
			}
		})
	}
}
