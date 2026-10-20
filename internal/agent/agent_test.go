package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/aenix-io/threek/internal/config"
	"github.com/aenix-io/threek/internal/etcdconf"
)

// TestRunNotImplemented documents the stub. Once implemented, Run polls the
// local member every PollInterval, feeds the reachability view to the
// decision engine and, in enforce mode, transfers leadership as described in
// docs/design.md, until ctx is done.
func TestRunNotImplemented(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Run(ctx, config.Default(), etcdconf.Settings{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("Run() error = %v, want ErrNotImplemented", err)
	}
}
