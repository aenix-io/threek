package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func TestDefault(t *testing.T) {
	want := Config{
		Mode:         ModeOff,
		PollInterval: time.Second,
		DownAfter:    3,
		UpAfter:      10,
		MoveAfter:    15 * time.Second,
		MoveCooldown: 2 * time.Minute,
		PreferAfter:  5 * time.Minute,
	}
	if got := Default(); got != want {
		t.Errorf("Default() = %+v, want %+v", got, want)
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		environ  []string
		want     func(*Config)
		wantArgs []string
		wantEnv  []string
	}{
		{
			name:     "nothing set",
			args:     []string{"--name=a", "--data-dir", "/var/lib/etcd"},
			environ:  []string{"PATH=/bin", "ETCD_NAME=a"},
			want:     func(*Config) {},
			wantArgs: []string{"--name=a", "--data-dir", "/var/lib/etcd"},
			wantEnv:  []string{"PATH=/bin", "ETCD_NAME=a"},
		},
		{
			name: "every flag, both syntaxes",
			args: []string{
				"--name=a",
				"--threek-mode=enforce",
				"--threek-poll-interval", "2s",
				"-threek-down-after=4",
				"--threek-up-after=6",
				"--threek-move-after=20s",
				"--threek-move-cooldown=1m",
				"--threek-identity=zone-a",
				"--threek-preferred-leader", "zone-c",
				"--threek-prefer-after=10m",
				"--threek-metrics-listen=127.0.0.1:2390",
				"--data-dir=/d",
			},
			want: func(c *Config) {
				c.Mode = ModeEnforce
				c.PollInterval = 2 * time.Second
				c.DownAfter = 4
				c.UpAfter = 6
				c.MoveAfter = 20 * time.Second
				c.MoveCooldown = time.Minute
				c.Identity = "zone-a"
				c.PreferredLeader = "zone-c"
				c.PreferAfter = 10 * time.Minute
				c.MetricsListen = "127.0.0.1:2390"
			},
			wantArgs: []string{"--name=a", "--data-dir=/d"},
		},
		{
			name:     "environment fallback is removed from etcd environment",
			environ:  []string{"PATH=/bin", "THREEK_MODE=dry-run", "THREEK_IDENTITY=node-1", "ETCD_NAME=a"},
			want:     func(c *Config) { c.Mode, c.Identity = ModeDryRun, "node-1" },
			wantArgs: []string{},
			wantEnv:  []string{"PATH=/bin", "ETCD_NAME=a"},
		},
		{
			name:     "flag wins over environment",
			args:     []string{"--threek-mode=enforce"},
			environ:  []string{"THREEK_MODE=dry-run"},
			want:     func(c *Config) { c.Mode = ModeEnforce },
			wantArgs: []string{},
			wantEnv:  []string{},
		},
		{
			name:     "last flag wins",
			args:     []string{"--threek-mode=enforce", "--threek-mode=dry-run"},
			want:     func(c *Config) { c.Mode = ModeDryRun },
			wantArgs: []string{},
		},
		{
			name:     "flags after the terminator are left alone",
			args:     []string{"--threek-mode=enforce", "--", "--threek-mode=off"},
			want:     func(c *Config) { c.Mode = ModeEnforce },
			wantArgs: []string{"--", "--threek-mode=off"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := Default()
			tt.want(&want)
			got, args, env, err := Parse(tt.args, tt.environ)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if got != want {
				t.Errorf("config = %+v, want %+v", got, want)
			}
			if !equalStrings(args, tt.wantArgs) {
				t.Errorf("etcd args = %q, want %q", args, tt.wantArgs)
			}
			if !equalStrings(env, tt.wantEnv) {
				t.Errorf("etcd env = %q, want %q", env, tt.wantEnv)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		environ []string
		want    string
	}{
		{name: "unknown flag", args: []string{"--threek-mod=enforce"}, want: `unknown flag "--threek-mod"`},
		{name: "unknown variable", environ: []string{"THREEK_MOD=enforce"}, want: `unknown environment variable "THREEK_MOD"`},
		{name: "missing value", args: []string{"--threek-mode"}, want: `flag "--threek-mode" needs a value`},
		{name: "bad mode", args: []string{"--threek-mode=on"}, want: `--threek-mode: must be one of off, dry-run, enforce`},
		{name: "bad duration", args: []string{"--threek-move-after=15"}, want: `--threek-move-after: time: missing unit`},
		{name: "bad duration from environment", environ: []string{"THREEK_MOVE_AFTER=x"}, want: `THREEK_MOVE_AFTER: time: invalid duration`},
		{name: "zero poll interval", args: []string{"--threek-poll-interval=0s"}, want: `--threek-poll-interval: must be positive`},
		{name: "negative cooldown", args: []string{"--threek-move-cooldown=-1s"}, want: `--threek-move-cooldown: must not be negative`},
		{name: "bad count", args: []string{"--threek-down-after=three"}, want: `--threek-down-after: must be an integer`},
		{name: "zero count", args: []string{"--threek-up-after=0"}, want: `--threek-up-after: must be at least 1`},
		{
			name: "preferred leader without identity",
			args: []string{"--threek-preferred-leader=zone-c"},
			want: `--threek-preferred-leader requires --threek-identity`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := Parse(tt.args, tt.environ)
			if err == nil {
				t.Fatalf("Parse() error = nil, want %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Parse() error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

func TestFromDir(t *testing.T) {
	tests := []struct {
		name  string
		files fstest.MapFS
		want  func(*Config)
	}{
		{
			name:  "empty directory of a missing optional ConfigMap",
			files: fstest.MapFS{},
			want:  func(*Config) {},
		},
		{
			name: "every shared setting",
			files: fstest.MapFS{
				"mode":             {Data: []byte("enforce")},
				"poll-interval":    {Data: []byte("2s")},
				"down-after":       {Data: []byte("4")},
				"up-after":         {Data: []byte("6")},
				"move-after":       {Data: []byte("20s")},
				"move-cooldown":    {Data: []byte("1m")},
				"preferred-leader": {Data: []byte("zone-c")},
				"prefer-after":     {Data: []byte("10m")},
			},
			want: func(c *Config) {
				c.Mode = ModeEnforce
				c.PollInterval = 2 * time.Second
				c.DownAfter = 4
				c.UpAfter = 6
				c.MoveAfter = 20 * time.Second
				c.MoveCooldown = time.Minute
				c.PreferredLeader = "zone-c"
				c.PreferAfter = 10 * time.Minute
			},
		},
		{
			name:  "surrounding whitespace of a YAML block scalar",
			files: fstest.MapFS{"mode": {Data: []byte("dry-run\n")}},
			want:  func(c *Config) { c.Mode = ModeDryRun },
		},
		{
			name: "kubelet bookkeeping entries",
			files: fstest.MapFS{
				"..data":                         {Data: []byte("x")},
				"..2026_10_21_12_00_00.1/mode":   {Data: []byte("enforce")},
				"mode":                           {Data: []byte("dry-run")},
				"..2026_10_21_12_00_00.1/ignore": {Data: []byte("x")},
			},
			want: func(c *Config) { c.Mode = ModeDryRun },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := Default()
			tt.want(&want)
			got, err := FromDir(tt.files)
			if err != nil {
				t.Fatalf("FromDir() error = %v", err)
			}
			if got != want {
				t.Errorf("config = %+v, want %+v", got, want)
			}
		})
	}
}

func TestFromDirErrors(t *testing.T) {
	tests := []struct {
		name  string
		files fstest.MapFS
		want  string
	}{
		{name: "unknown key", files: fstest.MapFS{"mod": {Data: []byte("enforce")}}, want: `unknown key "mod"`},
		{name: "bad value", files: fstest.MapFS{"mode": {Data: []byte("on")}}, want: `mode: must be one of off, dry-run, enforce`},
		{name: "identity differs per member", files: fstest.MapFS{"identity": {Data: []byte("zone-a")}}, want: `"identity" is set per member`},
		{name: "metrics address belongs to the container", files: fstest.MapFS{"metrics-listen": {Data: []byte(":2390")}}, want: `"metrics-listen" is set per member`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FromDir(tt.files)
			if err == nil {
				t.Fatalf("FromDir() error = nil, want %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("FromDir() error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

// kubelet mounts every key as a symlink through ..data to the current
// revision, and swaps ..data atomically on update.
func TestFromDirKubeletLayout(t *testing.T) {
	dir := t.TempDir()
	revision := filepath.Join(dir, "..2026_10_21_12_00_00.1")
	if err := os.Mkdir(revision, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(revision, "mode"), []byte("enforce"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(revision), filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..data", "mode"), filepath.Join(dir, "mode")); err != nil {
		t.Fatal(err)
	}
	got, err := FromDir(os.DirFS(dir))
	if err != nil {
		t.Fatalf("FromDir() error = %v", err)
	}
	if got.Mode != ModeEnforce {
		t.Errorf("mode = %s, want enforce", got.Mode)
	}
}

func TestSet(t *testing.T) {
	cfg := Default()
	if err := cfg.Set("mode", "enforce"); err != nil {
		t.Fatalf("Set(mode) error = %v", err)
	}
	if err := cfg.Set("move-after", "20s"); err != nil {
		t.Fatalf("Set(move-after) error = %v", err)
	}
	if cfg.Mode != ModeEnforce || cfg.MoveAfter != 20*time.Second {
		t.Errorf("config = %+v", cfg)
	}
	for name, value := range map[string]string{"mod": "enforce", "mode": "on", "identity": "zone-a"} {
		if err := cfg.Set(name, value); err == nil {
			t.Errorf("Set(%s, %s) error = nil", name, value)
		}
	}
}

func TestShared(t *testing.T) {
	want := []string{"mode", "poll-interval", "down-after", "up-after", "move-after", "move-cooldown", "preferred-leader", "prefer-after"}
	if got := Shared(); !reflect.DeepEqual(got, want) {
		t.Errorf("Shared() = %v, want %v", got, want)
	}
}
