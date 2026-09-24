package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_StreamAck_when_Basic(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0-0")
	dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	// 删 1 条 + 不存在的 ID 不计。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XACK", "s", "g", "100-1", "999-9"))
	require.Equal(t, int64(1), dispatch(r, "XPENDING", "s", "g").Elems[0].I)
	// 缺 key/组 → 0 非错；非法 ID 报错。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XACK", "missing", "g", "100-1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XACK", "s", "nogroup", "100-1"))
	got := dispatch(r, "XACK", "s", "g", "bad")
	require.Equal(t, protocol.KindError, got.Kind)
	got = dispatch(r, "XACK", "s", "g", "100-*")
	require.Equal(t, protocol.KindError, got.Kind)
}

func Test_StreamPending_when_Summary(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0-0")
	// 空 PEL 摘要：[0, nil, nil, []]。
	got := dispatch(r, "XPENDING", "s", "g")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Equal(t, int64(0), got.Elems[0].I)
	require.Equal(t, protocol.KindBulkString, got.Elems[1].Kind)
	require.True(t, got.Elems[1].Bulk == nil)
	dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	got = dispatch(r, "XPENDING", "s", "g")
	require.Equal(t, int64(2), got.Elems[0].I)
	require.Equal(t, "100-1", string(got.Elems[1].Bulk))
	require.Equal(t, "100-2", string(got.Elems[2].Bulk))
	owners := got.Elems[3].Elems
	require.Len(t, owners, 1)
	require.Equal(t, "c1", string(owners[0].Elems[0].Bulk))
	require.Equal(t, int64(2), owners[0].Elems[1].I)
	// 缺 key/组 → NOGROUP。
	got = dispatch(r, "XPENDING", "missing", "g")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOGROUP")
	got = dispatch(r, "XPENDING", "s", "nogroup")
	require.Equal(t, protocol.KindError, got.Kind)
}

func Test_StreamPending_when_Range(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XADD", "s", "100-3", "a", "3")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0-0")
	dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	got := dispatch(r, "XPENDING", "s", "g", "-", "+", "10")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 3)
	require.Equal(t, "100-1", string(got.Elems[0].Elems[0].Bulk))
	require.Equal(t, "c1", string(got.Elems[0].Elems[1].Bulk))
	require.Equal(t, int64(1), got.Elems[0].Elems[3].I)
	// COUNT 截断 / consumer 过滤 / 未知消费者空。
	require.Len(t, dispatch(r, "XPENDING", "s", "g", "-", "+", "1").Elems, 1)
	require.Len(t, dispatch(r, "XPENDING", "s", "g", "-", "+", "10", "c1").Elems, 3)
	require.Len(t, dispatch(r, "XPENDING", "s", "g", "-", "+", "10", "nobody").Elems, 0)
	// 缺 count → syntax error；count 非整数 → value 错误。
	got = dispatch(r, "XPENDING", "s", "g", "-", "+")
	require.Equal(t, protocol.KindError, got.Kind)
	got = dispatch(r, "XPENDING", "s", "g", "-", "+", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	// 排他边界。
	require.Len(t, dispatch(r, "XPENDING", "s", "g", "(100-1", "+", "10").Elems, 2)
	// 多余参数截断式忽略：consumer="COUNT" 无匹配 → 空。
	require.Len(t, dispatch(r, "XPENDING", "s", "g", "-", "+", "10", "COUNT", "2").Elems, 0)
}
