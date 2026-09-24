package datastruct

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZSetRoundtrip(t *testing.T) {
	z := map[string]float64{"a": 1.5, "b": -2, "c": 1.5}
	raw := EncodeZSet(z)
	got, enc, err := DecodeZSet(raw)
	require.NoError(t, err)
	require.Equal(t, ZSetEncodingListpack, enc)
	require.Equal(t, z, got)
}

func TestZSetAdaptiveSkiplist(t *testing.T) {
	many := make(map[string]float64, 200)
	for i := 0; i < 200; i++ {
		many["m"+strings.Repeat("y", i)] = float64(i)
	}
	_, enc, err := DecodeZSet(EncodeZSet(many))
	require.NoError(t, err)
	require.Equal(t, ZSetEncodingSkiplist, enc)

	wide := map[string]float64{strings.Repeat("w", 65): 1}
	_, enc, err = DecodeZSet(EncodeZSet(wide))
	require.NoError(t, err)
	require.Equal(t, ZSetEncodingSkiplist, enc)
}

func TestZSetTruncated(t *testing.T) {
	_, _, err := DecodeZSet(nil)
	require.Error(t, err)
	_, _, err = DecodeZSet([]byte{ZSetEncodingListpack, 0x02})
	require.Error(t, err)
}

func TestParseScoreRejects(t *testing.T) {
	for _, s := range []string{"nan", "NaN", "+inf", "-inf", "inf", "abc", ""} {
		_, err := ParseScore(s)
		require.Error(t, err, s)
	}
	f, err := ParseScore("1.5")
	require.NoError(t, err)
	require.Equal(t, 1.5, f)
}

func TestZSetKey(t *testing.T) {
	require.Equal(t, []byte("z:mykey"), ZSetKey("mykey"))
}
