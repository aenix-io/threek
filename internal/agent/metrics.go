package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metrics struct {
	registry        *prometheus.Registry
	mode            *prometheus.GaugeVec
	linkUp          *prometheus.GaugeVec
	isLeader        prometheus.Gauge
	transfers       *prometheus.CounterVec
	dryRunTransfers *prometheus.CounterVec
	records         prometheus.Gauge
	pollFailures    prometheus.Counter
	lastPoll        prometheus.Gauge
}

func newMetrics() *metrics {
	m := &metrics{
		registry: prometheus.NewRegistry(),
		mode: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "threek_mode",
			Help: "The agent's current mode: 1 for the active one. Other metrics describe only a mode other than off.",
		}, []string{"mode"}),
		linkUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "threek_link_up",
			Help: "Whether the link from one member to another is up after hysteresis: 1 up, 0 down or unknown.",
		}, []string{"from", "to"}),
		isLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threek_is_leader",
			Help: "Whether the local member is the leader.",
		}),
		transfers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "threek_transfers_total",
			Help: "Leadership transfers attempted, by reason and result.",
		}, []string{"reason", "result"}),
		dryRunTransfers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "threek_dry_run_transfers_total",
			Help: "Leadership transfers decided in dry-run mode, counted once per decision.",
		}, []string{"reason"}),
		records: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threek_member_records",
			Help: "Member records visible to this agent.",
		}),
		pollFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "threek_poll_failures_total",
			Help: "Polls that could not read the local member.",
		}),
		lastPoll: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "threek_last_poll_timestamp_seconds",
			Help: "Unix time of the last successful poll.",
		}),
	}
	m.registry.MustRegister(
		m.mode, m.linkUp, m.isLeader, m.transfers, m.dryRunTransfers, m.records, m.pollFailures, m.lastPoll,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// serve exposes the metrics on addr until ctx is done.
func (m *metrics) serve(ctx context.Context, addr string) (stop func(), err error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listener: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(l)
	}()
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			_ = srv.Close()
		}
		<-done
	}, nil
}
