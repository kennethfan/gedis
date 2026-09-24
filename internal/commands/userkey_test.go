package commands

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_RawToUserKey_when_Prefixed(t *testing.T) {
	cases := map[string]string{
		"s:k":   "k",
		"st:k":  "k",
		"h:f":   "f",
		"l:lst": "lst",
		"z:zs":  "zs",
		"x:stm": "stm",
		"hll:h": "h",
		"k":     "k",
	}
	for raw, want := range cases {
		require.Equal(t, want, RawToUserKey(raw), "raw=%q", raw)
	}
}

func Test_RawToUserKey_when_NestedPrefix(t *testing.T) {
	require.Equal(t, "s:x", RawToUserKey("s:s:x"))
	require.Equal(t, "", RawToUserKey("s:"))
}
