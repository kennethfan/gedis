package datastruct

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Given: an empty element slice
// When: encoding an empty list
// Then: decoding yields an empty list
func Test_ListCodec_when_Empty(t *testing.T) {
	enc := EncodeList(nil)

	elems, err := DecodeList(enc)
	require.NoError(t, err)
	require.Empty(t, elems)
}

// Given: a small list
// When: encoding it
// Then: ziplist encoding is chosen and round-trips in order
func Test_ListCodec_when_Small(t *testing.T) {
	elems := []string{"a", "b", "c"}
	enc := EncodeList(elems)

	require.Equal(t, ListEncodingZiplist, enc[0])
	got, err := DecodeList(enc)
	require.NoError(t, err)
	require.Equal(t, elems, got)
}

// Given: a list exceeding 128 elements
// When: encoding it
// Then: quicklist encoding is chosen and round-trips in order
func Test_ListCodec_when_Large(t *testing.T) {
	elems := make([]string, 0, 200)
	for i := range 200 {
		elems = append(elems, fmt.Sprintf("elem-%03d", i))
	}
	enc := EncodeList(elems)

	require.Equal(t, ListEncodingQuicklist, enc[0])
	got, err := DecodeList(enc)
	require.NoError(t, err)
	require.Equal(t, elems, got)
}

// Given: a single element larger than 64 bytes
// When: encoding it
// Then: quicklist encoding is chosen (mirrors hash adaptive rule)
func Test_ListCodec_when_BigElement(t *testing.T) {
	elems := []string{strings.Repeat("x", 65)}
	enc := EncodeList(elems)

	require.Equal(t, ListEncodingQuicklist, enc[0])
	got, err := DecodeList(enc)
	require.NoError(t, err)
	require.Equal(t, elems, got)
}

// Given: a truncated payload
// When: decoding it
// Then: an error is returned
func Test_ListCodec_when_Truncated(t *testing.T) {
	_, err := DecodeList([]byte{ListEncodingZiplist, 0x02})
	require.Error(t, err)
}

// Given: elements pushed at head and tail
// When: appending head then tail
// Then: order is preserved head-first
func Test_ListCodec_when_PushBothEnds(t *testing.T) {
	enc := EncodeList([]string{"b"})
	enc = ListPushHead(enc, "a")
	enc = ListPushTail(enc, "c")

	got, err := DecodeList(enc)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, got)
}
