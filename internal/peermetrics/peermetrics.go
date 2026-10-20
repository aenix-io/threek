// Package peermetrics reads etcd's Raft transport state from its /metrics.
package peermetrics

import (
	"fmt"
	"io"
	"strconv"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const (
	activePeers       = "etcd_network_active_peers"
	disconnectedPeers = "etcd_network_disconnected_peers_total"
)

// Peers is one member's view of its peers, keyed by peer member ID.
type Peers struct {
	Active      map[uint64]bool
	Disconnects map[uint64]float64
}

// Parse reads the series of member local from a /metrics response. Members
// running in one process share a registry, so series of other members may be
// present and are skipped.
func Parse(r io.Reader, local uint64) (Peers, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return Peers{}, fmt.Errorf("parse metrics: %w", err)
	}
	p := Peers{Active: map[uint64]bool{}, Disconnects: map[uint64]float64{}}
	err = forEach(families[activePeers], local, func(remote uint64, m *dto.Metric) {
		p.Active[remote] = m.GetGauge().GetValue() > 0
	})
	if err != nil {
		return Peers{}, err
	}
	err = forEach(families[disconnectedPeers], local, func(remote uint64, m *dto.Metric) {
		p.Disconnects[remote] = m.GetCounter().GetValue()
	})
	if err != nil {
		return Peers{}, err
	}
	return p, nil
}

func forEach(family *dto.MetricFamily, local uint64, fn func(remote uint64, m *dto.Metric)) error {
	for _, m := range family.GetMetric() {
		labels := map[string]string{}
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		from, err := strconv.ParseUint(labels["Local"], 16, 64)
		if err != nil {
			return fmt.Errorf("%s: Local label: %w", family.GetName(), err)
		}
		if from != local {
			continue
		}
		remote, err := strconv.ParseUint(labels["Remote"], 16, 64)
		if err != nil {
			return fmt.Errorf("%s: Remote label: %w", family.GetName(), err)
		}
		fn(remote, m)
	}
	return nil
}

// ActiveSince returns which peers count as active in this sample: a peer
// that disconnected since prev counts as inactive even if it is connected
// again, so flaps shorter than the poll interval are not lost. A zero prev
// means there was no previous sample.
func (p Peers) ActiveSince(prev Peers) map[uint64]bool {
	active := make(map[uint64]bool, len(p.Active))
	for remote, up := range p.Active {
		reconnected := prev.Disconnects != nil && p.Disconnects[remote] > prev.Disconnects[remote]
		active[remote] = up && !reconnected
	}
	return active
}
