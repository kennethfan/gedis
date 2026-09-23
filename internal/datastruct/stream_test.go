package datastruct

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamParseIDForms(t *testing.T) {
	id, auto, err := StreamParseID("5000-3")
	require.NoError(t, err)
	require.False(t, auto)
	require.Equal(t, StreamID{Ms: 5000, Seq: 3}, id)

	id, auto, err = StreamParseID("5000-*")
	require.NoError(t, err)
	require.True(t, auto)
	require.Equal(t, uint64(5000), id.Ms)

	_, auto, err = StreamParseID("*")
	require.NoError(t, err)
	require.True(t, auto)

	id, auto, err = StreamParseID("5000")
	require.NoError(t, err)
	require.False(t, auto)
	require.Equal(t, StreamID{Ms: 5000}, id)

	for _, bad := range []string{"", "bad", "1-2-3", "-5", "x-y", "18446744073709551616-0", "0--1"} {
		_, _, err := StreamParseID(bad)
		require.Error(t, err, bad)
	}
}

func TestStreamIDFormatCompare(t *testing.T) {
	require.Equal(t, "5000-3", StreamID{Ms: 5000, Seq: 3}.String())
	require.True(t, StreamID{Ms: 1, Seq: 9}.Less(StreamID{Ms: 2, Seq: 0}))
	require.True(t, StreamID{Ms: 2, Seq: 0}.Less(StreamID{Ms: 2, Seq: 1}))
	require.False(t, StreamID{Ms: 2, Seq: 1}.Less(StreamID{Ms: 2, Seq: 1}))
	require.Equal(t, 0, StreamID{Ms: 2, Seq: 1}.Compare(StreamID{Ms: 2, Seq: 1}))
}

func TestStreamCodecRoundtrip(t *testing.T) {
	st := StreamNew()
	st.Add(StreamID{Ms: 100, Seq: 1}, []string{"a", "1", "b", "2"})
	st.Add(StreamID{Ms: 100, Seq: 2}, []string{"c", "3"})
	st.Last = StreamID{Ms: 100, Seq: 2}
	st.Added = 2
	st.MaxDeleted = StreamID{Ms: 50, Seq: 0}
	g := &StreamGroup{Name: "g1", LastID: StreamID{Ms: 100, Seq: 1}, EntriesRead: 1, HasRead: true}
	g.Consumers = []string{"c1", "c2"}
	st.Groups = []*StreamGroup{g}
	raw := EncodeStream(st)
	got, err := DecodeStream(raw)
	require.NoError(t, err)
	require.Equal(t, st, got)
}

func TestStreamCodecTruncated(t *testing.T) {
	_, err := DecodeStream(nil)
	require.Error(t, err)
	_, err = DecodeStream([]byte{1, 2, 3})
	require.Error(t, err)
	raw := EncodeStream(StreamNew())
	raw[len(raw)-1] ^= 0xff
	_, err = DecodeStream(raw)
	require.Error(t, err)
}

func TestStreamKey(t *testing.T) {
	require.Equal(t, []byte("x:mykey"), StreamKey("mykey"))
}
