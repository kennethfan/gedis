package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_StreamGroup_when_ReadGroupNew(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XADD", "s", "100-3", "a", "3")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0-0")
	got := dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	pair := got.Elems[0].Elems
	require.Equal(t, "s", string(pair[0].Bulk))
	entries := pair[1].Elems
	require.Len(t, entries, 3)
	require.Equal(t, "100-1", string(entries[0].Elems[0].Bulk))
	require.Equal(t, "100-3", string(entries[2].Elems[0].Bulk))
	// 再读无新消息 → nil。
	require.Equal(t, protocol.Value{Kind: protocol.KindArray},
		dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"))
	// PEL 建了 3 条。
	require.Equal(t, int64(3), dispatch(r, "XPENDING", "s", "g").Elems[0].I)
}

func Test_StreamGroup_when_ReadGroupErrors(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	// 缺 key/组。
	got := dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "missing", ">")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOGROUP")
	got = dispatch(r, "XREADGROUP", "GROUP", "nogroup", "c1", "STREAMS", "s", ">")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOGROUP")
	// $ 非法。
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0-0")
	got = dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", "$")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "meaningless")
	// 参数不足 / 未知选项。
	got = dispatch(r, "XREADGROUP", "GROUP", "g")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "'xreadgroup'")
	// STREAMS 后仅 1 个 key（缺 ID）→ wrong-number；奇数个 → Unbalanced。
	got = dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments for 'xreadgroup'")
	got = dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "BOGUS", "1", "STREAMS", "s", ">")
	require.Equal(t, protocol.KindError, got.Kind)
}

func Test_StreamGroup_when_ExplicitHistory(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0-0")
	dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	// c2 显式读历史：他人 entries 跳过 → 流名 + 空数组（对标真 Redis）。
	got := dispatch(r, "XREADGROUP", "GROUP", "g", "c2", "STREAMS", "s", "0-0")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	require.Equal(t, "s", string(got.Elems[0].Elems[0].Bulk))
	require.Len(t, got.Elems[0].Elems[1].Elems, 0)
	// c1 显式重读：count+1。
	got = dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", "0-0")
	require.Len(t, got.Elems, 1)
	require.Len(t, got.Elems[0].Elems[1].Elems, 2)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XACK", "s", "g", "100-1"))
	require.Equal(t, int64(1), dispatch(r, "XPENDING", "s", "g").Elems[0].I)
}

func Test_StreamGroup_when_Noack(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0-0")
	got := dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "NOACK", "STREAMS", "s", ">")
	require.Len(t, got.Elems, 1)
	// NOACK 不建 PEL 但推进 last。
	require.Equal(t, int64(0), dispatch(r, "XPENDING", "s", "g").Elems[0].I)
	got = dispatch(r, "XINFO", "GROUPS", "s")
	require.Equal(t, "100-1", string(got.Elems[0].Elems[7].Bulk))
}
