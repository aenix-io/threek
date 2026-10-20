package config

import (
	"reflect"
	"strings"
	"testing"
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
