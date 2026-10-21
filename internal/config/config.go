// Package config holds threek settings and reads the shared ones from a
// mounted ConfigMap.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"
)

type Mode string

const (
	ModeOff     Mode = "off"
	ModeDryRun  Mode = "dry-run"
	ModeEnforce Mode = "enforce"
)

type Config struct {
	Mode            Mode
	PollInterval    time.Duration
	DownAfter       int
	UpAfter         int
	MoveAfter       time.Duration
	MoveCooldown    time.Duration
	Identity        string
	PreferredLeader string
	PreferAfter     time.Duration
	MetricsListen   string
}

func Default() Config {
	return Config{
		Mode:         ModeOff,
		PollInterval: time.Second,
		DownAfter:    3,
		UpAfter:      10,
		MoveAfter:    15 * time.Second,
		MoveCooldown: 2 * time.Minute,
		PreferAfter:  5 * time.Minute,
	}
}

type setting struct {
	name string
	set  func(c *Config, value string) error
}

// perMember settings differ between members of one cluster, so they cannot
// come from a ConfigMap shared by the cluster.
var perMember = map[string]bool{"identity": true, "metrics-listen": true}

var settings = []setting{
	{"mode", func(c *Config, v string) error {
		switch m := Mode(v); m {
		case ModeOff, ModeDryRun, ModeEnforce:
			c.Mode = m
			return nil
		}
		return errors.New("must be one of off, dry-run, enforce")
	}},
	{"poll-interval", duration(func(c *Config) *time.Duration { return &c.PollInterval }, false)},
	{"down-after", count(func(c *Config) *int { return &c.DownAfter })},
	{"up-after", count(func(c *Config) *int { return &c.UpAfter })},
	{"move-after", duration(func(c *Config) *time.Duration { return &c.MoveAfter }, false)},
	{"move-cooldown", duration(func(c *Config) *time.Duration { return &c.MoveCooldown }, true)},
	{"identity", text(func(c *Config) *string { return &c.Identity })},
	{"preferred-leader", text(func(c *Config) *string { return &c.PreferredLeader })},
	{"prefer-after", duration(func(c *Config) *time.Duration { return &c.PreferAfter }, false)},
	{"metrics-listen", text(func(c *Config) *string { return &c.MetricsListen })},
}

func duration(field func(*Config) *time.Duration, zeroAllowed bool) func(*Config, string) error {
	return func(c *Config, v string) error {
		d, err := time.ParseDuration(v)
		switch {
		case err != nil:
			return err
		case d < 0:
			return errors.New("must not be negative")
		case d == 0 && !zeroAllowed:
			return errors.New("must be positive")
		}
		*field(c) = d
		return nil
	}
}

func count(field func(*Config) *int) func(*Config, string) error {
	return func(c *Config, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return errors.New("must be an integer")
		}
		if n < 1 {
			return errors.New("must be at least 1")
		}
		*field(c) = n
		return nil
	}
}

func text(field func(*Config) *string) func(*Config, string) error {
	return func(c *Config, v string) error {
		*field(c) = v
		return nil
	}
}

func lookup(name string) (setting, bool) {
	for _, s := range settings {
		if s.name == name {
			return s, true
		}
	}
	return setting{}, false
}

// FromDir reads the settings shared by every member of a cluster from a
// mounted ConfigMap, one file per key. Settings without a file keep their
// defaults, so an empty directory means threek is off.
func FromDir(dir fs.FS) (Config, error) {
	cfg := Default()
	entries, err := fs.ReadDir(dir, ".")
	if err != nil {
		return Config{}, err
	}
	for _, e := range entries {
		// kubelet keeps the ConfigMap's revisions in "..*" entries and links
		// every key to the current one.
		if strings.HasPrefix(e.Name(), "..") {
			continue
		}
		value, err := fs.ReadFile(dir, e.Name())
		if err != nil {
			return Config{}, err
		}
		if err := cfg.Set(e.Name(), strings.TrimSpace(string(value))); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

// Shared returns the names of the settings shared by every member of a
// cluster, in the order of the configuration reference.
func Shared() []string {
	var names []string
	for _, s := range settings {
		if !perMember[s.name] {
			names = append(names, s.name)
		}
	}
	return names
}

// Set applies a shared setting by name.
func (c *Config) Set(name, value string) error {
	s, ok := lookup(name)
	switch {
	case !ok:
		return fmt.Errorf("unknown key %q", name)
	case perMember[name]:
		return fmt.Errorf("%q is set per member, not shared by the cluster", name)
	}
	if err := s.set(c, value); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}
