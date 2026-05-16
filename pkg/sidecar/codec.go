package sidecar

import (
	"fmt"

	rpc "github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// rawCodec wraps pkg/rpc's RawCodec so it satisfies grpc.encoding.Codec
// (which requires Name() string, vs the deprecated grpc.Codec's String()).
// The marshal/unmarshal behavior is identical: any *rpc.Frame is passed
// through as raw bytes; anything else falls back to proto.
type rawCodec struct{ inner rpc.RawCodec }

func newRawCodec() *rawCodec {
	return &rawCodec{inner: rpc.RawCodec{}}
}

func (c *rawCodec) Marshal(v interface{}) ([]byte, error) {
	// rpc.RawCodec needs a parentCodec; we route through the
	// pkg/rpc.Codec() factory so behavior tracks any future tweaks
	// there.
	rc := rpc.Codec().(*rpc.RawCodec)
	return rc.Marshal(v)
}

func (c *rawCodec) Unmarshal(data []byte, v interface{}) error {
	rc := rpc.Codec().(*rpc.RawCodec)
	return rc.Unmarshal(data, v)
}

func (c *rawCodec) Name() string {
	// Must be stable: a peer with a different name would reject our
	// messages. "proxy" matches what pkg/rpc.RawCodec.String() returns.
	return "proxy"
}

func (c *rawCodec) String() string { return fmt.Sprintf("sidecar/%s", c.Name()) }

// insecureTransport returns credentials suitable for loopback gRPC.
// The sidecar terminates app traffic on 127.0.0.1; TLS would be theatre.
func insecureTransport() credentials.TransportCredentials {
	return insecure.NewCredentials()
}
