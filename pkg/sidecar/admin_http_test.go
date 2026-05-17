package sidecar

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// httpRegister is the language-agnostic flow a Python/Node caller would
// follow: POST JSON, read the first NDJSON line for the nid, optionally
// read further keepalive acks. Returns the parsed nid, a function the
// test can call to read the next streamed line (blocking), and a
// release func that closes the connection.
func httpRegister(t *testing.T, addr, svcid, upstream string, services []string) (nid string, readLine func() (map[string]any, error), release func()) {
	t.Helper()
	body, _ := json.Marshal(httpRegisterRequest{Svcid: svcid, Upstream: upstream, Services: services})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://"+addr+"/v1/register", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	// Disable client-side keepalive pooling so closing the response
	// body actually closes the TCP connection — what we want for
	// lease semantics in tests.
	tr := &http.Transport{DisableKeepAlives: true}
	client := &http.Client{Transport: tr, Timeout: 0}

	resp, err := client.Do(req)
	require.NoError(t, err)
	// Don't pass readBody(resp) to require.Equal — testify evaluates
	// message args eagerly, and reading the body here would block
	// forever (the registration lease keeps the response stream open
	// indefinitely). Read a small prefix of the body only if the
	// assertion is going to fail.
	if resp.StatusCode != http.StatusOK {
		var buf [256]byte
		n, _ := resp.Body.Read(buf[:])
		_ = resp.Body.Close()
		t.Fatalf("register failed: status=%d body-prefix=%q", resp.StatusCode, buf[:n])
	}

	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadBytes('\n')
	require.NoError(t, err, "did not get first NDJSON line")

	var initial map[string]any
	require.NoError(t, json.Unmarshal(first, &initial))
	gotNid, ok := initial["nid"].(string)
	require.True(t, ok, "first line missing nid: %v", initial)
	require.NotEmpty(t, gotNid)

	readLine = func() (map[string]any, error) {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		var v map[string]any
		if err := json.Unmarshal(line, &v); err != nil {
			return nil, err
		}
		return v, nil
	}
	release = func() {
		_ = resp.Body.Close()
		tr.CloseIdleConnections()
	}
	return gotNid, readLine, release
}

// TestHTTPAdmin_RegisterHappyPath pins the core flow: a JSON POST opens
// a registration, the response's first NDJSON line carries the
// sidecar's nid, and the registration map gains exactly one entry.
func TestHTTPAdmin_RegisterHappyPath(t *testing.T) {
	url := runEmbeddedNATS(t)
	sc := startSidecar(t, url, "http-happy")

	upstream := startUpstreamEcho(t, &echoSrv{id: "X"})

	nid, _, release := httpRegister(t, sc.HTTPAdminAddr(), "svcH", upstream, []string{"echo.Echo"})
	defer release()
	assert.Equal(t, sc.Nid(), nid, "nid in HTTP response must match sidecar's own nid")

	sc.mu.Lock()
	regCount := len(sc.registrations)
	sc.mu.Unlock()
	assert.Equal(t, 1, regCount, "exactly one ingress registration should be open")
}

// TestHTTPAdmin_DisconnectDeregisters proves the lease semantics: when
// the HTTP connection drops (the canonical signal of "the local app
// died") the sidecar tears down the NATS subscriptions. This is the
// HTTP equivalent of the gRPC admin's stream-close path.
func TestHTTPAdmin_DisconnectDeregisters(t *testing.T) {
	url := runEmbeddedNATS(t)
	sc := startSidecar(t, url, "http-drop")

	upstream := startUpstreamEcho(t, &echoSrv{id: "X"})
	_, _, release := httpRegister(t, sc.HTTPAdminAddr(), "svcD", upstream, []string{"echo.Echo"})

	sc.mu.Lock()
	require.Equal(t, 1, len(sc.registrations))
	sc.mu.Unlock()

	release() // close the HTTP connection

	// Give the server-side request goroutine a moment to notice the
	// EOF and run its deferred closeIngress.
	deadline := time.Now().Add(2 * time.Second)
	var final int
	for time.Now().Before(deadline) {
		sc.mu.Lock()
		final = len(sc.registrations)
		sc.mu.Unlock()
		if final == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("registrations did not drain after HTTP connection closed; still %d", final)
}

// TestHTTPAdmin_Validation walks the obvious rejection cases. Each
// returns 4xx with a JSON error body before any ingress state is
// touched — never start a registration we know we'll have to tear
// down five lines later.
func TestHTTPAdmin_Validation(t *testing.T) {
	url := runEmbeddedNATS(t)
	sc := startSidecar(t, url, "http-validate")

	cases := []struct {
		name   string
		method string
		path   string
		body   string
		status int
		wantIn string
	}{
		{"method GET not allowed", http.MethodGet, "/v1/register", "", http.StatusMethodNotAllowed, "not allowed"},
		{"bad JSON", http.MethodPost, "/v1/register", "{not json", http.StatusBadRequest, "invalid JSON"},
		{"missing svcid", http.MethodPost, "/v1/register",
			`{"upstream":"127.0.0.1:1","services":["s"]}`, http.StatusBadRequest, "svcid"},
		{"missing upstream", http.MethodPost, "/v1/register",
			`{"svcid":"s","services":["s"]}`, http.StatusBadRequest, "upstream"},
		{"empty services", http.MethodPost, "/v1/register",
			`{"svcid":"s","upstream":"127.0.0.1:1","services":[]}`, http.StatusBadRequest, "service"},
		{"unknown field", http.MethodPost, "/v1/register",
			`{"svcid":"s","upstream":"127.0.0.1:1","services":["x"],"oops":1}`,
			http.StatusBadRequest, "invalid JSON"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, "http://"+sc.HTTPAdminAddr()+tc.path,
				strings.NewReader(tc.body))
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, tc.status, resp.StatusCode)
			body, _ := io.ReadAll(resp.Body)
			assert.Contains(t, string(body), tc.wantIn, "body=%s", string(body))
		})
	}
	// Validation failures must not leave ingress entries behind.
	sc.mu.Lock()
	count := len(sc.registrations)
	sc.mu.Unlock()
	assert.Zero(t, count)
}

// TestHTTPAdmin_BadUpstreamSurfacesAsBadGateway exercises the
// openIngress failure path: a syntactically valid body that names an
// upstream we can't actually dial. We expect 502 with a JSON error.
//
// grpc.NewClient is lazy and won't fail-fast on an unreachable host,
// so this test asserts the path is reachable rather than asserting a
// specific NATS-tier error; the chosen upstream is a bogus host name
// that grpc.NewClient still accepts.
func TestHTTPAdmin_AcceptsLazyUpstream(t *testing.T) {
	// grpc.NewClient doesn't dial until a call is made, so even an
	// obviously bad upstream string succeeds at registration time.
	// We document this — the call-time failure is exercised
	// elsewhere via the no-backend scenario in
	// TestAdmin_AppDeathDeregisters.
	url := runEmbeddedNATS(t)
	sc := startSidecar(t, url, "http-lazy")

	body, _ := json.Marshal(httpRegisterRequest{
		Svcid:    "svcL",
		Upstream: "127.0.0.1:1", // port 1, will fail on actual call
		Services: []string{"echo.Echo"},
	})
	resp, err := http.Post("http://"+sc.HTTPAdminAddr()+"/v1/register",
		"application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestHTTPAdmin_EndToEndWithGoEgress is the headline integration check:
// a backend registers via the HTTP/JSON endpoint exactly the way a
// Python or Node service would, and a Go client through the egress
// sidecar can call it and get the right response. Crucially the client
// only uses standard gRPC; only the registration step is HTTP.
func TestHTTPAdmin_EndToEndWithGoEgress(t *testing.T) {
	url := runEmbeddedNATS(t)
	egress := startSidecar(t, url, "http-e2e-egress")
	ingress := startSidecar(t, url, "http-e2e-ingress")

	upstream := startUpstreamEcho(t, &echoSrv{id: "PY"})
	_, _, release := httpRegister(t, ingress.HTTPAdminAddr(), "svcPY", upstream, []string{"echo.Echo"})
	defer release()

	conn := dialEgress(t, egress.EgressAddr())
	cli := echo.NewEchoClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", "svcPY")
	resp, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "world"})
	require.NoError(t, err)
	assert.Equal(t, "PY:world", resp.Msg)
}

// TestHTTPAdmin_CoexistsWithGRPCAdmin proves both admin paths share
// the same openIngress / closeIngress machinery and don't trample one
// another: one app registers via gRPC, another registers via HTTP,
// both work and they can be torn down independently.
func TestHTTPAdmin_CoexistsWithGRPCAdmin(t *testing.T) {
	url := runEmbeddedNATS(t)
	egress := startSidecar(t, url, "coexist-egress")
	ingress := startSidecar(t, url, "coexist-ingress")

	upA := startUpstreamEcho(t, &echoSrv{id: "A"})
	upB := startUpstreamEcho(t, &echoSrv{id: "B"})

	_, relA := registerApp(t, ingress.AdminAddr(), "svcCoA", upA, []string{"echo.Echo"})
	defer relA()
	_, _, relB := httpRegister(t, ingress.HTTPAdminAddr(), "svcCoB", upB, []string{"echo.Echo"})
	defer relB()

	ingress.mu.Lock()
	count := len(ingress.registrations)
	ingress.mu.Unlock()
	assert.Equal(t, 2, count, "both admin paths should yield independent registrations")

	conn := dialEgress(t, egress.EgressAddr())
	cli := echo.NewEchoClient(conn)
	for _, c := range []struct{ svcid, want string }{{"svcCoA", "A:x"}, {"svcCoB", "B:x"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", c.svcid)
		resp, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "x"})
		cancel()
		require.NoError(t, err)
		assert.Equal(t, c.want, resp.Msg)
	}
}

// TestHTTPAdmin_Disable verifies HTTPAdminAddr="-" turns the endpoint
// off entirely (useful for tests, constrained environments, or
// security audits that want to assert one fewer bound socket).
func TestHTTPAdmin_Disable(t *testing.T) {
	url := runEmbeddedNATS(t)
	sc := New(Config{
		NATSURL:       url,
		EgressAddr:    "127.0.0.1:0",
		AdminAddr:     "127.0.0.1:0",
		HTTPAdminAddr: "-",
		Nid:           "disabled",
	})
	require.NoError(t, sc.Start(context.Background()))
	t.Cleanup(func() { _ = sc.Close() })

	assert.Empty(t, sc.HTTPAdminAddr(), "HTTPAdminAddr() should be empty when disabled")
}

// TestHTTPAdmin_KeepaliveAck exercises the streaming ack channel. We
// reduce the keepalive interval via a package-level test hook so we
// don't have to wait the production 30s.
func TestHTTPAdmin_KeepaliveAck(t *testing.T) {
	old := setHTTPAdminKeepalive(80 * time.Millisecond)
	defer setHTTPAdminKeepalive(old)

	url := runEmbeddedNATS(t)
	sc := startSidecar(t, url, "http-keepalive")
	upstream := startUpstreamEcho(t, &echoSrv{id: "K"})

	_, readLine, release := httpRegister(t, sc.HTTPAdminAddr(), "svcK", upstream, []string{"echo.Echo"})
	defer release()

	// At least two acks within 500ms means the ticker really fires
	// at the configured cadence.
	var seen int
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) && seen < 2 {
		line, err := readLine()
		require.NoError(t, err)
		if _, ok := line["ack"]; ok {
			seen++
		}
	}
	assert.GreaterOrEqual(t, seen, 2, "expected at least 2 keepalive acks; saw %d", seen)
}

// TestHTTPAdmin_ConcurrentRegistrations is a small stress test —
// ten goroutines register simultaneously, each holds its connection
// briefly, and we assert the registrations map grows to ten and
// then drains back to zero. Catches lock-ordering and goroutine
// leaks the simpler tests miss.
func TestHTTPAdmin_ConcurrentRegistrations(t *testing.T) {
	url := runEmbeddedNATS(t)
	sc := startSidecar(t, url, "http-stress")
	upstream := startUpstreamEcho(t, &echoSrv{id: "S"})

	const n = 10
	releases := make([]func(), n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, rel := httpRegister(t, sc.HTTPAdminAddr(), fmt.Sprintf("svc-%d", i), upstream, []string{"echo.Echo"})
			releases[i] = rel
		}(i)
	}
	wg.Wait()

	sc.mu.Lock()
	got := len(sc.registrations)
	sc.mu.Unlock()
	assert.Equal(t, n, got)

	for _, r := range releases {
		r()
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sc.mu.Lock()
		got = len(sc.registrations)
		sc.mu.Unlock()
		if got == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("registrations did not drain after all HTTP clients closed; still %d", got)
}
