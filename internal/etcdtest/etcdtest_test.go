package etcdtest

import (
	"net"
	"strings"
	"testing"
)

// Another process can take a port between freeAddr and the etcd bind.
func TestStartRetriesWhenAPortIsTaken(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = taken.Close() })

	handedOut := false
	addr := func(t *testing.T) string {
		if !handedOut {
			handedOut = true
			return taken.Addr().String()
		}
		return freeAddr(t)
	}

	cl := start(t, 3, addr)
	for _, m := range cl.Members {
		for _, u := range []string{m.ClientURL, m.PeerURL} {
			if strings.HasSuffix(u, taken.Addr().String()) {
				t.Errorf("%s listens on the taken address %s", m.Name, u)
			}
		}
	}
	if cl.Leader(t) == nil {
		t.Fatal("no leader")
	}
}
