package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ParseSlotRanges_when_Mixed(t *testing.T) {
	r, err := ParseSlotRanges([]string{"0-5460", "7001", " 10923-16383 "})
	require.NoError(t, err)
	require.Equal(t, [][2]int{{0, 5460}, {7001, 7001}, {10923, 16383}}, r)
}

func Test_ParseSlotRanges_when_Bad(t *testing.T) {
	for _, specs := range [][]string{
		{"-1"}, {"16384"}, {"10-5"}, {"0-16384"}, {"x"}, {"1-2-3"}, {""},
	} {
		_, err := ParseSlotRanges(specs)
		require.Error(t, err, "%v", specs)
	}
}

func Test_DeriveID_when_StableHex40(t *testing.T) {
	id := DeriveID("127.0.0.1:7000")
	require.Len(t, id, 40)
	require.Equal(t, id, DeriveID("127.0.0.1:7000"))
	require.NotEqual(t, id, DeriveID("127.0.0.1:7001"))
	for _, c := range id {
		require.Contains(t, "0123456789abcdef", string(c))
	}
}
