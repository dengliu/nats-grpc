package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestRandInt_RangeAndError(t *testing.T) {
	for i := 0; i < 1000; i++ {
		v, err := RandInt(10, 20)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, v, 10)
		assert.Less(t, v, 20)
	}

	// min == max and min > max are rejected — the old (math/rand) helper
	// silently returned `max` instead of erroring, masking caller bugs.
	_, err := RandInt(5, 5)
	assert.Error(t, err)
	_, err = RandInt(10, 5)
	assert.Error(t, err)
}

func TestGenerateRandomBytes_Length(t *testing.T) {
	b, err := GenerateRandomBytes(32)
	require.NoError(t, err)
	assert.Len(t, b, 32)

	// Two consecutive draws must differ — the old math/rand-seeded version
	// produced identical sequences when called in the same nanosecond.
	b2, err := GenerateRandomBytes(32)
	require.NoError(t, err)
	assert.NotEqual(t, b, b2, "two crypto/rand draws should not be identical")
}

func TestGenerateRandomString_LengthAndAlphabet(t *testing.T) {
	s, err := GenerateRandomString(40)
	require.NoError(t, err)
	assert.Len(t, s, 40)

	const allowed = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
	in := make(map[byte]struct{}, len(allowed))
	for i := 0; i < len(allowed); i++ {
		in[allowed[i]] = struct{}{}
	}
	for i := 0; i < len(s); i++ {
		_, ok := in[s[i]]
		assert.True(t, ok, "char %q at %d outside the URL-safe alphabet", s[i], i)
	}
}

func TestMakeMetadata_NilOrEmpty(t *testing.T) {
	assert.Nil(t, MakeMetadata(nil))
	assert.Nil(t, MakeMetadata(metadata.MD{}))
}

func TestMakeMetadata_RoundTrip(t *testing.T) {
	md := metadata.MD{
		"x-a": []string{"1", "2"},
		"x-b": []string{"only"},
	}
	out := MakeMetadata(md)
	require.NotNil(t, out)
	require.NotNil(t, out.Md)
	assert.Equal(t, []string{"1", "2"}, out.Md["x-a"].Values)
	assert.Equal(t, []string{"only"}, out.Md["x-b"].Values)
}
