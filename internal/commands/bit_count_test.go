package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: "\xf0\x0f"（bits: 0-3 置位，12-15 置位）
// When: BITCOUNT 全模式
// Then: 与真 Redis 仲裁值一致
func Test_Bitmap_when_BitCount(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterBitmap(r, store)
	dispatch(r, "SET", "bc", "\xf0\x0f")
	require.Equal(t, int64(8), dispatch(r, "BITCOUNT", "bc").I)
	require.Equal(t, int64(4), dispatch(r, "BITCOUNT", "bc", "0", "0").I)
	require.Equal(t, int64(8), dispatch(r, "BITCOUNT", "bc", "0", "-1").I)
	require.Equal(t, int64(4), dispatch(r, "BITCOUNT", "bc", "1", "1", "BYTE").I)
	require.Equal(t, int64(4), dispatch(r, "BITCOUNT", "bc", "0", "7", "BIT").I)
	require.Equal(t, int64(0), dispatch(r, "BITCOUNT", "bc", "4", "7", "BIT").I)
	require.Equal(t, int64(4), dispatch(r, "BITCOUNT", "bc", "-8", "-1", "BIT").I)
	require.Equal(t, int64(4), dispatch(r, "BITCOUNT", "bc", "5", "30", "BIT").I)
	require.Equal(t, int64(8), dispatch(r, "BITCOUNT", "bc", "0", "-1", "BIT").I)
	require.Equal(t, int64(4), dispatch(r, "BITCOUNT", "bc", "8", "15", "BIT").I)
	require.Equal(t, int64(0), dispatch(r, "BITCOUNT", "nosuch").I)
	require.Equal(t, int64(0), dispatch(r, "BITCOUNT", "nosuch", "0", "-1").I)
	require.Equal(t, protocol.KindError, dispatch(r, "BITCOUNT", "bc", "x", "y").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "BITCOUNT", "bc", "0", "1", "BITS").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "BITCOUNT", "bc", "0").Kind)
}

// Given: "\xf0\x0f"
// When: BITPOS 全模式
// Then: 与真 Redis 仲裁值一致
func Test_Bitmap_when_BitPos(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterBitmap(r, store)
	dispatch(r, "SET", "bc", "\xf0\x0f")
	require.Equal(t, int64(4), dispatch(r, "BITPOS", "bc", "0").I)
	require.Equal(t, int64(0), dispatch(r, "BITPOS", "bc", "1").I)
	require.Equal(t, int64(12), dispatch(r, "BITPOS", "bc", "1", "1").I)
	require.Equal(t, int64(4), dispatch(r, "BITPOS", "bc", "0", "0", "-1").I)
	require.Equal(t, int64(-1), dispatch(r, "BITPOS", "bc", "0", "2", "3").I)
	require.Equal(t, int64(-1), dispatch(r, "BITPOS", "bc", "1", "4", "7", "BIT").I)
	require.Equal(t, int64(4), dispatch(r, "BITPOS", "bc", "0", "0", "7", "BIT").I)
	require.Equal(t, int64(8), dispatch(r, "BITPOS", "bc", "0", "8", "15", "BIT").I)
	require.Equal(t, int64(12), dispatch(r, "BITPOS", "bc", "1", "8", "15", "BIT").I)
	require.Equal(t, int64(0), dispatch(r, "BITPOS", "nosuch", "0").I)
	require.Equal(t, int64(-1), dispatch(r, "BITPOS", "nosuch", "1").I)
	require.Equal(t, protocol.KindError, dispatch(r, "BITPOS", "bc", "2").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "BITPOS", "bc", "0", "x").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "BITPOS", "bc").Kind)
}
