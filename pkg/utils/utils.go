package utils

import (
	"crypto/rand"
	"encoding/binary"
	"errors"

	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/metadata"
)

// RandInt returns a uniformly-distributed integer in [min, max). It is
// backed by crypto/rand so it is safe for tokens / IDs.
func RandInt(min, max int) (int, error) {
	if min >= max {
		return 0, errors.New("RandInt: min must be < max")
	}
	span := uint64(max - min)
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0, err
	}
	n := binary.BigEndian.Uint64(raw[:]) % span
	return min + int(n), nil
}

// GenerateRandomBytes returns n cryptographically-random bytes.
func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// GenerateRandomString returns a URL-safe random string of length n. The
// alphabet is the same 64-char set NATS uses for inbox subjects.
func GenerateRandomString(n int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
	bytes, err := GenerateRandomBytes(n)
	if err != nil {
		return "", err
	}
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes), nil
}

func NewInBox() string {
	return nats.NewInbox()
}

func MakeMetadata(md metadata.MD) *nrpc.Metadata {
	if md == nil || md.Len() == 0 {
		return nil
	}
	result := make(map[string]*nrpc.Strings, md.Len())
	for key, values := range md {
		result[key] = &nrpc.Strings{
			Values: values,
		}
	}
	return &nrpc.Metadata{
		Md: result,
	}
}
