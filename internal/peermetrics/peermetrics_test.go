package peermetrics

import (
	"maps"
	"strings"
	"testing"
)

const (
	local  uint64 = 0x8e9e05c52164694d
	peerB  uint64 = 0x91bc3c398fb3c146
	peerC  uint64 = 0xfd422379fda50e48
	sample        = `# HELP etcd_network_active_peers The current number of active peer connections.
# TYPE etcd_network_active_peers gauge
etcd_network_active_peers{Local="8e9e05c52164694d",Remote="91bc3c398fb3c146"} 1
etcd_network_active_peers{Local="8e9e05c52164694d",Remote="fd422379fda50e48"} 0
etcd_network_active_peers{Local="91bc3c398fb3c146",Remote="8e9e05c52164694d"} 1
# HELP etcd_network_disconnected_peers_total The total number of disconnected peers.
# TYPE etcd_network_disconnected_peers_total counter
etcd_network_disconnected_peers_total{Local="8e9e05c52164694d",Remote="fd422379fda50e48"} 2
etcd_network_disconnected_peers_total{Local="91bc3c398fb3c146",Remote="8e9e05c52164694d"} 7
# HELP etcd_server_has_leader Whether or not a leader exists. 1 is existence, 0 is not.
# TYPE etcd_server_has_leader gauge
etcd_server_has_leader 1
`
)

func TestParse(t *testing.T) {
	got, err := Parse(strings.NewReader(sample), local)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	wantActive := map[uint64]bool{peerB: true, peerC: false}
	if !maps.Equal(got.Active, wantActive) {
		t.Errorf("Active = %v, want %v", got.Active, wantActive)
	}
	wantDisconnects := map[uint64]float64{peerC: 2}
	if !maps.Equal(got.Disconnects, wantDisconnects) {
		t.Errorf("Disconnects = %v, want %v", got.Disconnects, wantDisconnects)
	}
}

func TestParseWithoutPeerMetrics(t *testing.T) {
	got, err := Parse(strings.NewReader("# TYPE etcd_server_has_leader gauge\netcd_server_has_leader 1\n"), local)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(got.Active) != 0 || len(got.Disconnects) != 0 {
		t.Errorf("Parse() = %+v, want no peers", got)
	}
}

func TestParseErrors(t *testing.T) {
	for name, text := range map[string]string{
		"malformed":       "etcd_network_active_peers{Local=\"8e9e05c52164694d\" 1\n",
		"bad remote":      "etcd_network_active_peers{Local=\"8e9e05c52164694d\",Remote=\"xyz\"} 1\n",
		"missing remote":  "etcd_network_active_peers{Local=\"8e9e05c52164694d\"} 1\n",
		"bad counter key": "etcd_network_disconnected_peers_total{Local=\"8e9e05c52164694d\",Remote=\"\"} 1\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(strings.NewReader(text), local); err == nil {
				t.Error("Parse() error = nil")
			}
		})
	}
}

func TestActive(t *testing.T) {
	prev := Peers{Disconnects: map[uint64]float64{peerB: 1, peerC: 3}}
	cur := Peers{
		Active:      map[uint64]bool{peerB: true, peerC: true},
		Disconnects: map[uint64]float64{peerB: 2, peerC: 3},
	}
	// peerB reconnected between polls, so this sample counts as inactive.
	want := map[uint64]bool{peerB: false, peerC: true}
	if got := cur.ActiveSince(prev); !maps.Equal(got, want) {
		t.Errorf("ActiveSince() = %v, want %v", got, want)
	}
}

func TestActiveFirstPoll(t *testing.T) {
	cur := Peers{
		Active:      map[uint64]bool{peerB: true},
		Disconnects: map[uint64]float64{peerB: 5},
	}
	want := map[uint64]bool{peerB: true}
	if got := cur.ActiveSince(Peers{}); !maps.Equal(got, want) {
		t.Errorf("ActiveSince() = %v, want %v", got, want)
	}
}
