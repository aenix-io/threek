package agent

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aenix-io/threek/internal/config"
	"github.com/aenix-io/threek/internal/etcdtest"
)

func TestPreferredLeaderOnRealCluster(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	leader := cl.Leader(t)
	var preferred *etcdtest.Member
	for _, m := range cl.Members {
		if m != leader {
			preferred = m
			break
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var agents sync.WaitGroup
	defer agents.Wait()
	defer cancel()
	for _, m := range cl.Members {
		cfg := config.Default()
		cfg.Mode = config.ModeEnforce
		cfg.PollInterval = 200 * time.Millisecond
		cfg.UpAfter = 2
		cfg.MoveCooldown = 0
		cfg.PreferAfter = 2 * time.Second
		cfg.Identity = "zone-" + m.Name
		cfg.PreferredLeader = "zone-" + preferred.Name
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		agents.Go(func() {
			if err := Run(ctx, cfg, m.Settings, logger); err != nil {
				t.Errorf("agent of %s: %v", m.Name, err)
			}
		})
	}

	deadline := time.Now().Add(30 * time.Second)
	for cl.Leader(t) != preferred {
		if time.Now().After(deadline) {
			t.Fatalf("leader is %s, want the preferred member %s", cl.Leader(t).Name, preferred.Name)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
