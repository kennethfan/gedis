package datastruct

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_HashCodec_when_Small(t *testing.T) {
	m := map[string]string{"f1": "v1", "f2": "v2"}
	raw := EncodeHash(m)
	require.Equal(t, EncodingListpack, raw[0])

	got, enc, err := DecodeHash(raw)
	require.NoError(t, err)
	require.Equal(t, EncodingListpack, enc)
	require.Equal(t, m, got)
}

func Test_HashCodec_when_ManyFields(t *testing.T) {
	m := make(map[string]string, 200)
	for i := 0; i < 200; i++ {
		m[fmt.Sprintf("f%d", i)] = "v"
	}
	raw := EncodeHash(m)
	require.Equal(t, EncodingHashtable, raw[0])

	got, enc, err := DecodeHash(raw)
	require.NoError(t, err)
	require.Equal(t, EncodingHashtable, enc)
	require.Equal(t, m, got)
}

func Test_HashCodec_when_BigValue(t *testing.T) {
	big := make([]byte, 65)
	for i := range big {
		big[i] = 'x'
	}
	m := map[string]string{"f": string(big)}
	raw := EncodeHash(m)
	require.Equal(t, EncodingHashtable, raw[0])
}

func Test_HashCodec_when_Truncated(t *testing.T) {
	_, _, err := DecodeHash([]byte{EncodingListpack, 0x01})
	require.Error(t, err)
}

func Test_HashKey_when_Prefix(t *testing.T) {
	require.Equal(t, []byte("h:myhash"), HashKey("myhash"))
}
