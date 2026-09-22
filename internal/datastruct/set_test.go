package datastruct

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_SetCodec_when_IntMembers(t *testing.T) {
	members := []string{"3", "1", "2", "-5", "10"}

	raw := EncodeSet(members)

	got, enc, err := DecodeSet(raw)
	require.NoError(t, err)
	require.Equal(t, EncodingIntset, enc)
	require.Len(t, got, 5)
	for _, m := range members {
		_, ok := got[m]
		require.True(t, ok, "member %q missing", m)
	}
}

func Test_SetCodec_when_MixedMembers(t *testing.T) {
	members := []string{"1", "hello", "3"}

	raw := EncodeSet(members)

	got, enc, err := DecodeSet(raw)
	require.NoError(t, err)
	require.Equal(t, EncodingHashtable, enc)
	require.Len(t, got, 3)
}

func Test_SetCodec_when_TooManyIntMembers(t *testing.T) {
	members := make([]string, 0, 513)
	for i := 0; i < 513; i++ {
		members = append(members, fmt.Sprintf("%d", i))
	}

	raw := EncodeSet(members)

	got, enc, err := DecodeSet(raw)
	require.NoError(t, err)
	require.Equal(t, EncodingHashtable, enc)
	require.Len(t, got, 513)
}

func Test_SetCodec_when_Empty(t *testing.T) {
	raw := EncodeSet(nil)

	got, enc, err := DecodeSet(raw)
	require.NoError(t, err)
	require.Equal(t, EncodingIntset, enc)
	require.Empty(t, got)
}

func Test_SetCodec_when_Duplicates(t *testing.T) {
	raw := EncodeSet([]string{"a", "b", "a", "c", "b"})

	got, enc, err := DecodeSet(raw)
	require.NoError(t, err)
	require.Equal(t, EncodingHashtable, enc)
	require.Len(t, got, 3)
}

func Test_SetCodec_when_Truncated(t *testing.T) {
	_, _, err := DecodeSet([]byte{})
	require.Error(t, err)

	raw := EncodeSet([]string{"a", "b"})
	_, _, err = DecodeSet(raw[:len(raw)-1])
	require.Error(t, err)
}

func Test_SetKey_when_Prefix(t *testing.T) {
	require.Equal(t, []byte("st:myset"), SetKey("myset"))
}
