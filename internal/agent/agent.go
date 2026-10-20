// Package agent watches the local etcd member and transfers leadership.
package agent

import (
	"context"
	"errors"
	"log/slog"

	"github.com/aenix-io/threek/internal/config"
	"github.com/aenix-io/threek/internal/etcdconf"
)

var ErrNotImplemented = errors.New("agent is not implemented yet")

// Run blocks until ctx is done.
func Run(ctx context.Context, cfg config.Config, _ etcdconf.Settings, logger *slog.Logger) error {
	logger.Warn("agent is not implemented yet, etcd runs without leader placement", "mode", cfg.Mode)
	<-ctx.Done()
	return ErrNotImplemented
}
