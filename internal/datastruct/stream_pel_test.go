package datastruct

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamPELRoundtrip(t *testing.T) {
	st := StreamNew()
	st.Add(StreamID{Ms: 100, Seq: 1}, []string{"a", "1"})
	st.Last = StreamID{Ms: 100, Seq: 1}
	st.HasLast = true
	st.Added = 1
	g := &StreamGroup{Name: "g", LastID: StreamID{Ms: 100, Seq: 1}}
	g.PEL = []*StreamPEL{
		{ID: StreamID{Ms: 100, Seq: 1}, Consumer: "c1", DeliveryMs: 1700000000000, Count: 2},
	}
	g.Consumers = []StreamConsumer{{Name: "c1", SeenMs: 1700000000000}}
	st.Groups = []*StreamGroup{g}
	got, err := DecodeStream(EncodeStream(st))
	require.NoError(t, err)
	require.Equal(t, st, got)
}

func TestStreamPELDecodeLegacy(t *testing.T) {
	// M2-1 老 payload（无 PEL 段）仍能解码，PEL 为空。
	st := StreamNew()
	st.Add(StreamID{Ms: 100, Seq: 1}, []string{"a", "1"})
	raw := EncodeStream(st)
	require.NotEmpty(t, raw)
	got, err := DecodeStream(raw)
	require.NoError(t, err)
	require.Empty(t, got.Groups)
}

func TestStreamPELOps(t *testing.T) {
	g := &StreamGroup{Name: "g"}
	require.Nil(t, g.FindPEL(StreamID{Ms: 1, Seq: 0}))
	g.UpsertPEL(&StreamPEL{ID: StreamID{Ms: 100, Seq: 2}, Consumer: "c1", DeliveryMs: 10, Count: 1})
	g.UpsertPEL(&StreamPEL{ID: StreamID{Ms: 100, Seq: 1}, Consumer: "c2", DeliveryMs: 20, Count: 3})
	// 有序插入：100-1 在前。
	require.Equal(t, StreamID{Ms: 100, Seq: 1}, g.PEL[0].ID)
	require.Equal(t, StreamID{Ms: 100, Seq: 2}, g.PEL[1].ID)
	// 同 ID 替换。
	g.UpsertPEL(&StreamPEL{ID: StreamID{Ms: 100, Seq: 1}, Consumer: "c1", DeliveryMs: 30, Count: 4})
	require.Len(t, g.PEL, 2)
	require.Equal(t, "c1", g.PEL[0].Consumer)
	require.Equal(t, uint64(4), g.PEL[0].Count)
	// 删除。
	require.True(t, g.DelPEL(StreamID{Ms: 100, Seq: 1}))
	require.False(t, g.DelPEL(StreamID{Ms: 100, Seq: 1}))
	require.Len(t, g.PEL, 1)
}
