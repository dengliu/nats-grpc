package rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubjects_LegacyMatchesHistoricalLayout(t *testing.T) {
	// The legacy layout must remain bit-for-bit compatible with the
	// inlined builder it replaced — otherwise existing deployed peers
	// can't talk to each other across an upgrade.
	assert.Equal(t, "nrpc.svc-1.echo.Echo.SayHello",
		legacySubject("svc-1", "/echo.Echo/SayHello"))
	assert.Equal(t, "nrpc.echo.Echo.SayHello",
		legacySubject("", "/echo.Echo/SayHello"))
}

func TestSubjects_ModernUnary(t *testing.T) {
	assert.Equal(t, "nrpc.unary.svc-1.echo.Echo.SayHello",
		modernUnarySubject("svc-1", "/echo.Echo/SayHello"))
	// Multi-segment svcid is allowed; NATS subject tokens are
	// dot-separated and svcid contributes one segment per dot.
	assert.Equal(t, "nrpc.unary.tenant.payments.echo.Echo.SayHello",
		modernUnarySubject("tenant.payments", "/echo.Echo/SayHello"))
}

func TestSubjects_ModernStream(t *testing.T) {
	assert.Equal(t, "nrpc.stream.svc-1.replica-a.echo.Echo.Echo",
		modernStreamSubject("svc-1", "replica-a", "/echo.Echo/Echo"))
}

func TestSubjects_ModernUnaryQueueGroup(t *testing.T) {
	assert.Equal(t, "u:svc-1:echo.Echo",
		modernUnaryQueueGroup("svc-1", "echo.Echo"))
}
