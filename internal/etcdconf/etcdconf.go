// Package etcdconf describes how the agent reaches the local etcd member.
package etcdconf

// Settings name the local member's client URL and the files the agent
// authenticates with.
type Settings struct {
	// Endpoint is the local member's client URL. The server certificates
	// issued by Talos and etcd-operator cover loopback addresses.
	Endpoint          string
	CertFile          string
	KeyFile           string
	TrustedCAFile     string
	PeerCertFile      string
	PeerKeyFile       string
	PeerTrustedCAFile string
}
