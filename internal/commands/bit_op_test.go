package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: k1="\xff\xff" k2="\x0f"
// When: BITOP 全操作 + 缺失源 + 非法参数
// Then: 结果长度取最长输入，缺失按零填充，与真 Redis 仲裁值一致
func Test_Bitmap_when_BitOp(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	RegisterBitmap(r, store)
	dispatch(r, "SET", "k1", "\xff\xff")
	dispatch(r, "SET", "k2", "\x0f")

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "BITOP", "AND", "da", "k1", "k2"))
	require.Equal(t, protocol.BulkOf("\x0f\x00"), dispatch(r, "GET", "da"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "BITOP", "OR", "do", "k1", "k2"))
	require.Equal(t, protocol.BulkOf("\xff\xff"), dispatch(r, "GET", "do"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "BITOP", "XOR", "dx", "k1", "k2"))
	require.Equal(t, protocol.BulkOf("\xf0\xff"), dispatch(r, "GET", "dx"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "BITOP", "NOT", "dn", "k2"))
	require.Equal(t, protocol.BulkOf("\xf0"), dispatch(r, "GET", "dn"))

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "BITOP", "AND", "dm", "k1", "nosuch"))
	require.Equal(t, protocol.BulkOf("\x00\x00"), dispatch(r, "GET", "dm"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "BITOP", "OR", "dmo", "k1", "nosuch"))
	require.Equal(t, protocol.BulkOf("\xff\xff"), dispatch(r, "GET", "dmo"))

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "BITOP", "OR", "de", "nosuch1", "nosuch2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "EXISTS", "de"))
	dispatch(r, "SET", "dd", "old")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "BITOP", "AND", "dd", "nosuch1", "nosuch2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "EXISTS", "dd"))

	require.Equal(t, protocol.KindError, dispatch(r, "BITOP", "NOT", "dn2", "k1", "k2").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "BITOP", "DIFF", "d", "k1").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "BITOP", "AND", "d").Kind)

	dispatch(r, "HSET", "h", "f", "v")
	require.Equal(t, protocol.KindError, dispatch(r, "BITOP", "AND", "d", "h", "k1").Kind)
}
