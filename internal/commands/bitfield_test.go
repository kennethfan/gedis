package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 空库
// When: BITFIELD 混合读写 + OVERFLOW 模式切换
// Then: 与真 Redis 仲裁值一致
func Test_Bitfield_when_MixedOps(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterBitmap(r, store)
	got := dispatch(r, "BITFIELD", "bf", "SET", "u8", "#0", "200", "GET", "u8", "0", "INCRBY", "u8", "0", "56", "GET", "u8", "0")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		{Kind: protocol.KindInteger, I: 0},
		{Kind: protocol.KindInteger, I: 200},
		{Kind: protocol.KindInteger, I: 0},
		{Kind: protocol.KindInteger, I: 0},
	}}, got)

	dispatch(r, "BITFIELD", "bf", "SET", "u8", "#0", "200")
	got = dispatch(r, "BITFIELD", "bf", "OVERFLOW", "SAT", "INCRBY", "u8", "#0", "100")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 255}, got.Elems[0])
	got = dispatch(r, "BITFIELD", "bf", "OVERFLOW", "FAIL", "INCRBY", "u8", "#0", "100")
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, got.Elems[0])

	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		{Kind: protocol.KindInteger, I: 0},
		{Kind: protocol.KindInteger, I: 15},
	}}, dispatch(r, "BITFIELD", "bh", "SET", "u4", "#1", "15", "GET", "u4", "#1"))
}

// Given: 空库
// When: BITFIELD 非法输入
// Then: 错误文本与真 Redis 一致
func Test_Bitfield_when_Errors(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	RegisterBitmap(r, store)
	require.Equal(t, protocol.Value{Kind: protocol.KindError, S: "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."},
		dispatch(r, "BITFIELD", "x", "SET", "u64", "#0", "1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError, S: "ERR bit offset is not an integer or out of range"},
		dispatch(r, "BITFIELD", "x", "GET", "u8", "-1"))
	require.Equal(t, protocol.KindError, dispatch(r, "BITFIELD", "x", "FOO", "u8", "0").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "BITFIELD", "x", "GET", "u8").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "BITFIELD", "x", "GET", "u8", "0", "EXTRA").Kind)
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}},
		dispatch(r, "BITFIELD", "x"))
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}},
		dispatch(r, "BITFIELD", "x", "OVERFLOW", "SAT"))
	dispatch(r, "HSET", "h", "f", "v")
	require.Equal(t, protocol.KindError, dispatch(r, "BITFIELD", "h", "GET", "u8", "0").Kind)
}
