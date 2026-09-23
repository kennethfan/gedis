package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 空库
// When: SETBIT/GETBIT 读写 + 越界/非法参数
// Then: 大端 bit 序，自动零扩展，错误与真 Redis 一致
func Test_Bitmap_when_SetGetBit(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterBitmap(r, store)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SETBIT", "b", "7", "1"))
	require.Equal(t, protocol.BulkOf("\x01"), dispatch(r, "GET", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SETBIT", "b", "0", "1"))
	require.Equal(t, protocol.BulkOf("\x81"), dispatch(r, "GET", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SETBIT", "b", "0", "0"))
	require.Equal(t, protocol.BulkOf("\x01"), dispatch(r, "GET", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "GETBIT", "b", "7"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "GETBIT", "b", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "GETBIT", "b", "100"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "GETBIT", "nosuch", "5"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "STRLEN", "nosuch"))
	require.Equal(t, protocol.KindError, dispatch(r, "SETBIT", "b", "-1", "1").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "SETBIT", "b", "4294967296", "1").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "SETBIT", "b", "1", "2").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "GETBIT", "b", "-1").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "SETBIT", "b", "1").Kind)
}

// Given: 已有字符串 "AB"
// When: 按 bit 视图读写
// Then: 与 string 视图同一份字节（A=0x41 首 bit 为 0，B=0x42 第二字节首 bit 为 0…）
func Test_Bitmap_when_StringViewConsistent(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterBitmap(r, store)
	dispatch(r, "SET", "s", "AB")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "GETBIT", "s", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "GETBIT", "s", "1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "GETBIT", "s", "8"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "GETBIT", "s", "9"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SETBIT", "s", "9", "0"))
	require.Equal(t, protocol.BulkOf("A\x02"), dispatch(r, "GET", "s"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "STRLEN", "s"))
}

// Given: 非 string key
// When: bitmap 读写
// Then: WRONGTYPE
func Test_Bitmap_when_WrongType(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	RegisterBitmap(r, store)
	dispatch(r, "HSET", "h", "f", "v")
	require.Equal(t, protocol.KindError, dispatch(r, "SETBIT", "h", "0", "1").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "GETBIT", "h", "0").Kind)
}
