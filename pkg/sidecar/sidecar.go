// Package sidecar implements a language-agnostic bridge between standard
// gRPC and the nats-grpc protocol. See SIDECAR.md for the design.
//
// The local app speaks vanilla gRPC to two loopback ports:
//   - Egress (default 127.0.0.1:50051): outbound RPCs go here. Each call
//     must include the x-nats-svcid metadata header naming the target
//     backend svcid. For streaming RPCs add x-nats-mode: streaming and
//     x-nats-target-nid: <replica>.
//   - Admin  (default 127.0.0.1:50100): the local app registers its
//     ingress (svcid + service list + upstream gRPC addr) here at
//     startup via the SidecarAdmin.Register bidi stream.
//
// Everything else — discovery, retries, auth — is out of scope for v1.
package sidecar

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"

	rpc "github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
)

// Config configures a Sidecar. All addresses default to loopback so a
// stock-config sidecar exposes nothing outside the pod.
type Config struct {
	// NATSURL is the NATS server URL (e.g. nats://nats:4222).
	NATSURL string
	// NATSOptions are passed verbatim to nats.Connect. Use this for TLS,
	// auth, custom timeouts, etc.
	NATSOptions []nats.Option

	// EgressAddr is the loopback gRPC address the local app dials for
	// outbound RPCs. Default: 127.0.0.1:50051.
	EgressAddr string
	// AdminAddr is the loopback gRPC address the local app uses to call
	// SidecarAdmin.Register. Default: 127.0.0.1:50100.
	AdminAddr string

	// Nid identifies this sidecar instance. It becomes the <nid> segment
	// of streaming subjects the sidecar subscribes to on behalf of
	// registered apps, so peers targeting "this replica" via
	// x-nats-target-nid use it. Defaults to a random hex string.
	Nid string
}

// Sidecar is the running sidecar process. Start opens listeners and the
// NATS connection; Close tears everything down.
type Sidecar struct {
	cfg Config
	nc  *nats.Conn
	cli *rpc.Client // shared nrpc client used by the egress path

	egressLis   net.Listener
	adminLis    net.Listener
	egressSrv   *grpc.Server
	adminSrv    *grpc.Server

	// registrations tracks live admin sessions. Keyed by an opaque
	// session ID; each entry owns its NATS subscriptions and an upstream
	// grpc.ClientConn.
	mu            sync.Mutex
	registrations map[string]*registration

	// streams indexes active ingress streamWorkers by their reply inbox.
	// Each inbound NATS frame on a streaming subject is dispatched here
	// so frames belonging to the same caller land in the same worker.
	streamsMu sync.Mutex
	streams   map[string]*streamWorker

	// done is closed when the sidecar shuts down. Used by the admin
	// stream-loop and ingress dispatchers to bail out.
	done chan struct{}
}

// New constructs a Sidecar from cfg. It does not open any sockets or
// connect to NATS — call Start.
func New(cfg Config) *Sidecar {
	if cfg.EgressAddr == "" {
		cfg.EgressAddr = "127.0.0.1:50051"
	}
	if cfg.AdminAddr == "" {
		cfg.AdminAddr = "127.0.0.1:50100"
	}
	if cfg.Nid == "" {
		cfg.Nid = randomNid()
	}
	return &Sidecar{
		cfg:           cfg,
		registrations: make(map[string]*registration),
		streams:       make(map[string]*streamWorker),
		done:          make(chan struct{}),
	}
}

// Start opens the NATS connection and both gRPC listeners. The returned
// errors come from listener / NATS setup; per-call failures surface to
// the local app as gRPC status errors.
func (s *Sidecar) Start(ctx context.Context) error {
	if s.cfg.NATSURL == "" {
		return errors.New("sidecar: NATSURL is required")
	}
	nc, err := nats.Connect(s.cfg.NATSURL, s.cfg.NATSOptions...)
	if err != nil {
		return fmt.Errorf("sidecar: connect NATS: %w", err)
	}
	s.nc = nc
	// One nrpc client per sidecar; per-call svcid is supplied via
	// InvokeWithSvcID / NewStreamWithSvcID rather than client state.
	s.cli = rpc.NewClient(nc, "" /* svcid unused on modern path */, s.cfg.Nid)

	if s.egressLis, err = net.Listen("tcp", s.cfg.EgressAddr); err != nil {
		s.nc.Close()
		return fmt.Errorf("sidecar: listen egress: %w", err)
	}
	if s.adminLis, err = net.Listen("tcp", s.cfg.AdminAddr); err != nil {
		_ = s.egressLis.Close()
		s.nc.Close()
		return fmt.Errorf("sidecar: listen admin: %w", err)
	}

	s.egressSrv = newEgressServer(s)
	s.adminSrv = newAdminServer(s)

	go func() { _ = s.egressSrv.Serve(s.egressLis) }()
	go func() { _ = s.adminSrv.Serve(s.adminLis) }()

	return nil
}

// EgressAddr / AdminAddr return the bound listener addresses, useful when
// the config specified port 0 (let-OS-pick).
func (s *Sidecar) EgressAddr() string { return s.egressLis.Addr().String() }
func (s *Sidecar) AdminAddr() string  { return s.adminLis.Addr().String() }

// Nid returns the sidecar's nid (auto-generated or from Config).
func (s *Sidecar) Nid() string { return s.cfg.Nid }

// Close shuts the sidecar down. Returns the first error encountered;
// best-effort cleans up the rest regardless.
func (s *Sidecar) Close() error {
	close(s.done)

	s.mu.Lock()
	regs := make([]*registration, 0, len(s.registrations))
	for _, r := range s.registrations {
		regs = append(regs, r)
	}
	s.registrations = map[string]*registration{}
	s.mu.Unlock()
	for _, r := range regs {
		r.teardown()
	}

	if s.egressSrv != nil {
		s.egressSrv.GracefulStop()
	}
	if s.adminSrv != nil {
		s.adminSrv.GracefulStop()
	}
	if s.cli != nil {
		_ = s.cli.Close()
	}
	if s.nc != nil {
		s.nc.Close()
	}
	return nil
}

func randomNid() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "sc-" + hex.EncodeToString(b[:])
}
