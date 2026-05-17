package sidecar

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// HTTP/JSON admin — language-agnostic registration endpoint:
//
//	POST /v1/register   body: {"svcid","upstream","services"}
//	→  200 OK, single NDJSON line: {"nid":"…"}
//	   then the response stream stays open with no further bytes.
//
// The open HTTP connection IS the lease. When the client (or the
// network) drops it, the sidecar tears down the NATS subscriptions
// and aborts in-flight ingress RPCs. No application-level heartbeat
// is needed: the documented deployment is a Kubernetes pod-mate over
// loopback, where TCP-level connection close is detected immediately.
//
// Validation failures (missing required fields, bad JSON, wrong method,
// wrong path) return ordinary 4xx responses with a small JSON error
// body. There is no authentication — the endpoint binds to loopback.

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

	// Hold the connection — it IS the lease. <-r.Context().Done() is
	// the idiomatic net/http way to wait for a client disconnect: the
	// stdlib cancels Request.Context() when the underlying TCP
	// connection closes (process exit, app crash, network tear-down).
	// The select adds a second case for sidecar shutdown since s.done
	// is a plain channel rather than a context. Either path runs the
	// deferred closeIngress above and we deregister.
	select {
	case <-r.Context().Done():
	case <-s.done:
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
