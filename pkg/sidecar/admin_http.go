package sidecar

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// HTTP/JSON admin — an ergonomic alternative to the gRPC SidecarAdmin
// for languages where running protoc against sidecar.proto is friction
// (Python, Node, Ruby, …). Same semantics as the gRPC admin:
//
//	POST /v1/register   body: {"svcid","upstream","services"}
//	→  200 OK, NDJSON streaming response:
//	      first line:  {"nid":"…"}
//	      subsequent:  {"ack":<server_unix_nano>}     every keepaliveInterval
//	The open HTTP connection IS the lease — when the client (or the
//	network) drops it, the sidecar tears down the NATS subscriptions
//	and aborts in-flight ingress RPCs, just like the gRPC admin path.
//
// Validation failures (missing required fields, bad JSON, wrong method,
// wrong path) return ordinary 4xx responses with a small JSON error
// body. There is no authentication — the endpoint binds to loopback
// for the same reason the gRPC admin does.

// httpAdminKeepaliveInterval is how often we emit an {"ack":…} line on
// the open NDJSON stream. The interval has two jobs: keep idle middle
// boxes (NAT, proxies) from dropping the connection, and surface
// hung-write conditions to the server-side write goroutine so we
// notice a dead client even when the client side never sends anything.
//
// Mutable so tests can dial it down — guarded by a package-level
// mutex even though concurrent test mutation isn't a real risk,
// because the production read happens on a request goroutine and
// running -race would otherwise complain.
var (
	httpAdminKeepaliveMu       sync.Mutex
	httpAdminKeepaliveInterval = 30 * time.Second
)

func getHTTPAdminKeepalive() time.Duration {
	httpAdminKeepaliveMu.Lock()
	defer httpAdminKeepaliveMu.Unlock()
	return httpAdminKeepaliveInterval
}

// setHTTPAdminKeepalive is a test-only hook. Returns the previous
// value so tests can restore it via defer.
func setHTTPAdminKeepalive(d time.Duration) time.Duration {
	httpAdminKeepaliveMu.Lock()
	defer httpAdminKeepaliveMu.Unlock()
	prev := httpAdminKeepaliveInterval
	httpAdminKeepaliveInterval = d
	return prev
}

type httpRegisterRequest struct {
	Svcid    string   `json:"svcid"`
	Upstream string   `json:"upstream"`
	Services []string `json:"services"`
}

type httpRegisterError struct {
	Error string `json:"error"`
}

// newHTTPAdminServer builds the *http.Server that handles /v1/register.
// Kept separate from newAdminServer so the gRPC admin remains
// independently usable; the two endpoints share openIngress under the
// hood so behaviour stays in sync.
func newHTTPAdminServer(s *Sidecar) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/register", s.handleHTTPRegister)
	return &http.Server{
		Handler: mux,
		// Read header timeout guards against slowloris; the request
		// body is small JSON so a few seconds is generous.
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func (s *Sidecar) handleHTTPRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeHTTPError(w, http.StatusMethodNotAllowed,
			fmt.Sprintf("method %s not allowed; use POST", r.Method))
		return
	}

	var req httpRegisterRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Svcid == "" {
		writeHTTPError(w, http.StatusBadRequest, "svcid is required")
		return
	}
	if req.Upstream == "" {
		writeHTTPError(w, http.StatusBadRequest, "upstream is required")
		return
	}
	if len(req.Services) == 0 {
		writeHTTPError(w, http.StatusBadRequest, "at least one service is required")
		return
	}

	// We need to flush each NDJSON line as a discrete chunk so the
	// client sees the nid promptly. Plain net/http supports this via
	// the Flusher interface, which standard ResponseWriters implement.
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeHTTPError(w, http.StatusInternalServerError, "streaming not supported by this server")
		return
	}

	reg, err := s.openIngress(req.Svcid, req.Upstream, req.Services)
	if err != nil {
		// openIngress returns gRPC status errors; surface the message
		// in a JSON body with a generic 400 — the typical failure
		// is "upstream unreachable" or a validation mistake, both
		// of which the caller can fix.
		writeHTTPError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer s.closeIngress(reg)

	// 200 OK + NDJSON. Setting Content-Type before the first write
	// is mandatory; once we write anything we lose the ability to
	// change status codes.
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if err := writeJSONLine(w, map[string]any{"nid": s.cfg.Nid}); err != nil {
		// Connection died before we could announce nid. openIngress's
		// defer above tears the registration back down.
		return
	}
	flusher.Flush()

	// Hold the connection as the lease. We loop on either:
	//   - r.Context().Done() — connection closed (client gone, sidecar
	//     shutting down, ReadTimeout elapsed, etc.). Either way we
	//     deregister via the deferred closeIngress.
	//   - keepaliveInterval — write a small {"ack":…} line. If the
	//     write fails (typically EPIPE / connection reset), bail
	//     and let the defer run.
	ticker := time.NewTicker(getHTTPAdminKeepalive())
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			if err := writeJSONLine(w, map[string]any{"ack": time.Now().UnixNano()}); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeJSONLine(w http.ResponseWriter, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}

func writeHTTPError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, _ := json.Marshal(httpRegisterError{Error: message})
	_, _ = w.Write(body)
}

// startHTTPAdmin opens the listener and serves in a background goroutine.
// Returns errors from listener creation only; per-request failures
// surface as HTTP responses.
func (s *Sidecar) startHTTPAdmin() error {
	lis, err := net.Listen("tcp", s.cfg.HTTPAdminAddr)
	if err != nil {
		return fmt.Errorf("listen http admin: %w", err)
	}
	s.httpAdminLis = lis
	s.httpAdminSrv = newHTTPAdminServer(s)
	go func() {
		if err := s.httpAdminSrv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Best-effort: the only realistic failure here is the
			// listener being closed during shutdown, which surfaces
			// as ErrServerClosed and is benign.
		}
	}()
	return nil
}
