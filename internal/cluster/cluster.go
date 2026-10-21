// Package cluster talks to the members of the local etcd cluster.
package cluster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/aenix-io/threek/internal/etcdconf"
)

const (
	// RecordPrefix is where member records live in the cluster's keyspace.
	RecordPrefix        = "/threek/members/"
	RecordSchemaVersion = 1

	dialTimeout = 5 * time.Second
)

var (
	ErrNotLeader    = errors.New("local member is not the leader")
	ErrLeaseExpired = errors.New("lease expired")
)

type Status struct {
	ClusterID    uint64
	MemberID     uint64
	Leader       uint64
	Term         uint64
	AppliedIndex uint64
	Errors       []string
}

type Member struct {
	ID         uint64
	Name       string
	ClientURLs []string
	PeerURLs   []string
	IsLearner  bool
}

// Record is what an agent publishes about its member. Readers ignore unknown
// fields, so newer agents may add fields within a minor version.
type Record struct {
	SchemaVersion int    `json:"schemaVersion"`
	Identity      string `json:"identity,omitempty"`
	AgentVersion  string `json:"agentVersion,omitempty"`
	// Sees reports whether this member reaches each peer, keyed by the peer's
	// member ID in hex.
	Sees map[string]bool `json:"sees,omitempty"`
}

// Client keeps one connection per member and authenticates with the local
// member's own certificate.
type Client struct {
	localURL string
	tls      *tls.Config

	mu      sync.Mutex
	clients map[string]*clientv3.Client
}

func New(s etcdconf.Settings) (*Client, error) {
	localURL := s.Endpoint
	var tlsConfig *tls.Config
	if strings.HasPrefix(localURL, "https://") {
		if s.TrustedCAFile == "" {
			return nil, errors.New("etcd serves TLS but has no trusted CA file to verify it with")
		}
		info := transport.TLSInfo{CertFile: s.CertFile, KeyFile: s.KeyFile, TrustedCAFile: s.TrustedCAFile}
		var err error
		if tlsConfig, err = info.ClientConfig(); err != nil {
			return nil, fmt.Errorf("client TLS: %w", err)
		}
	}
	return &Client{
		localURL: localURL,
		tls:      tlsConfig,
		clients:  map[string]*clientv3.Client{},
	}, nil
}

func (c *Client) LocalURL() string {
	return c.localURL
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for _, cli := range c.clients {
		errs = append(errs, cli.Close())
	}
	c.clients = map[string]*clientv3.Client{}
	return errors.Join(errs...)
}

// client returns the connection to the member at url. Connections are kept:
// clientv3's Maintenance.Status dials a new one on every call.
func (c *Client) client(url string) (*clientv3.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cli, ok := c.clients[url]; ok {
		return cli, nil
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{url},
		TLS:         c.tls,
		DialTimeout: dialTimeout,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", url, err)
	}
	c.clients[url] = cli
	return cli, nil
}

func (c *Client) Status(ctx context.Context, url string) (Status, error) {
	cli, err := c.client(url)
	if err != nil {
		return Status{}, err
	}
	resp, err := pb.NewMaintenanceClient(cli.ActiveConnection()).Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return Status{}, fmt.Errorf("status of %s: %w", url, rpctypes.Error(err))
	}
	return Status{
		ClusterID:    resp.GetHeader().GetClusterId(),
		MemberID:     resp.GetHeader().GetMemberId(),
		Leader:       resp.GetLeader(),
		Term:         resp.GetRaftTerm(),
		AppliedIndex: resp.GetRaftAppliedIndex(),
		Errors:       resp.GetErrors(),
	}, nil
}

// Members lists the cluster's members as the local member knows them, without
// a round trip to the leader.
func (c *Client) Members(ctx context.Context) ([]Member, error) {
	cli, err := c.client(c.localURL)
	if err != nil {
		return nil, err
	}
	resp, err := cli.MemberList(ctx, clientv3.WithSerializable())
	if err != nil {
		return nil, fmt.Errorf("member list: %w", err)
	}
	members := make([]Member, 0, len(resp.Members))
	for _, m := range resp.Members {
		members = append(members, Member{
			ID:         m.GetID(),
			Name:       m.GetName(),
			ClientURLs: m.GetClientURLs(),
			PeerURLs:   m.GetPeerURLs(),
			IsLearner:  m.GetIsLearner(),
		})
	}
	return members, nil
}

// MoveLeader transfers leadership from the local member to target. It
// returns ErrNotLeader when the local member is not the leader.
func (c *Client) MoveLeader(ctx context.Context, target uint64) error {
	cli, err := c.client(c.localURL)
	if err != nil {
		return err
	}
	if _, err := cli.MoveLeader(ctx, target); err != nil {
		if errors.Is(err, rpctypes.ErrNotLeader) {
			return ErrNotLeader
		}
		return fmt.Errorf("move leader to %x: %w", target, err)
	}
	return nil
}

// Records returns the member records keyed by member ID. Entries that are
// not valid records are skipped.
func (c *Client) Records(ctx context.Context) (map[uint64]Record, error) {
	cli, err := c.client(c.localURL)
	if err != nil {
		return nil, err
	}
	resp, err := cli.Get(ctx, RecordPrefix, clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return nil, fmt.Errorf("read member records: %w", err)
	}
	records := make(map[uint64]Record, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		id, err := strconv.ParseUint(strings.TrimPrefix(string(kv.Key), RecordPrefix), 16, 64)
		if err != nil {
			continue
		}
		var r Record
		if json.Unmarshal(kv.Value, &r) != nil {
			continue
		}
		records[id] = r
	}
	return records, nil
}

func recordKey(id uint64) string {
	return RecordPrefix + strconv.FormatUint(id, 16)
}

// GrantLease grants a lease for member records.
func (c *Client) GrantLease(ctx context.Context, ttl time.Duration) (int64, error) {
	cli, err := c.client(c.localURL)
	if err != nil {
		return 0, err
	}
	resp, err := cli.Grant(ctx, int64(ttl/time.Second))
	if err != nil {
		return 0, fmt.Errorf("grant record lease: %w", err)
	}
	return int64(resp.ID), nil
}

// PutRecord publishes r for member id under lease.
func (c *Client) PutRecord(ctx context.Context, id uint64, r Record, lease int64) error {
	cli, err := c.client(c.localURL)
	if err != nil {
		return err
	}
	value, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := cli.Put(ctx, recordKey(id), string(value), clientv3.WithLease(clientv3.LeaseID(lease))); err != nil {
		return fmt.Errorf("put member record: %w", err)
	}
	return nil
}

// RenewLease extends lease once. It returns ErrLeaseExpired when the lease
// no longer exists.
func (c *Client) RenewLease(ctx context.Context, lease int64) error {
	cli, err := c.client(c.localURL)
	if err != nil {
		return err
	}
	if _, err := cli.KeepAliveOnce(ctx, clientv3.LeaseID(lease)); err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return ErrLeaseExpired
		}
		return fmt.Errorf("renew record lease: %w", err)
	}
	return nil
}

// RevokeLease revokes lease, deleting the records attached to it.
func (c *Client) RevokeLease(ctx context.Context, lease int64) error {
	cli, err := c.client(c.localURL)
	if err != nil {
		return err
	}
	if _, err := cli.Revoke(ctx, clientv3.LeaseID(lease)); err != nil {
		return fmt.Errorf("revoke record lease: %w", err)
	}
	return nil
}
