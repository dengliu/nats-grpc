package rpc

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

// TestValidateStreamingNid is a unit test of the pure validator behind the
// RegisterService warning. It pins the safety contract for the streaming
// protocol: a service with streaming methods must be registered against a
// non-empty nid, otherwise multiple replicas will silently fan out stream
// frames via the NATS queue group.
func TestValidateStreamingNid(t *testing.T) {
	streaming := &grpc.ServiceDesc{
		ServiceName: "pkg.Service",
		Streams: []grpc.StreamDesc{
			{StreamName: "S", ClientStreams: true, ServerStreams: true},
		},
	}
	unaryOnly := &grpc.ServiceDesc{
		ServiceName: "pkg.UnaryOnly",
		Methods: []grpc.MethodDesc{
			{MethodName: "M"},
		},
	}

	tests := []struct {
		name    string
		sd      *grpc.ServiceDesc
		nid     string
		wantWarn bool
	}{
		{"streaming + empty nid warns", streaming, "", true},
		{"streaming + non-empty nid is fine", streaming, "replica-1", false},
		{"unary-only + empty nid is fine", unaryOnly, "", false},
		{"unary-only + non-empty nid is fine", unaryOnly, "replica-1", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := validateStreamingNid(tc.sd, tc.nid)
			if tc.wantWarn {
				assert.NotEmpty(t, got)
				assert.Contains(t, got, "pkg.Service")
				assert.Contains(t, got, "fan out")
				assert.True(t, strings.HasPrefix(got, "RegisterService"))
			} else {
				assert.Empty(t, got, "got unexpected warning: %s", got)
			}
		})
	}
}
