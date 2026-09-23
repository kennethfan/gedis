package commands

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// 缝线 B：BITFIELD 纯算术（类型/位移解析、读写、溢出），真值来自真 Redis 仲裁。

func Test_Bitfield_when_ParseType(t *testing.T) {
	ty, err := parseBitfieldType("u8")
	require.NoError(t, err)
	require.Equal(t, bitfieldType{signed: false, bits: 8}, ty)
	ty, err = parseBitfieldType("i64")
	require.NoError(t, err)
	require.Equal(t, bitfieldType{signed: true, bits: 64}, ty)
	ty, err = parseBitfieldType("U1")
	require.NoError(t, err)
	require.Equal(t, bitfieldType{signed: false, bits: 1}, ty)
	for _, bad := range []string{"u0", "u64", "i0", "i65", "x8", "u", "", "u08x"} {
		_, err := parseBitfieldType(bad)
		require.Error(t, err, bad)
	}
}

func Test_Bitfield_when_ParseOffset(t *testing.T) {
	off, err := parseBitfieldOffset("0", 8)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = parseBitfieldOffset("#1", 4)
	require.NoError(t, err)
	require.Equal(t, uint64(4), off)
	off, err = parseBitfieldOffset("4294967295", 1)
	require.NoError(t, err)
	require.Equal(t, uint64(4294967295), off)
	for _, bad := range []string{"-1", "#-1", "4294967296", "x", ""} {
		_, err := parseBitfieldOffset(bad, 8)
		require.Error(t, err, bad)
	}
}

func Test_Bitfield_when_GetSet(t *testing.T) {
	u8 := bitfieldType{signed: false, bits: 8}
	require.Equal(t, int64(0), bitfieldGet(nil, 0, u8))
	old, out := bitfieldSet(nil, 0, u8, 200)
	require.Equal(t, int64(0), old)
	require.Equal(t, []byte{200}, out)
	require.Equal(t, int64(200), bitfieldGet(out, 0, u8))
	// SET 隐式截断（仲裁：SET u8 300 → 存 44）
	old, out = bitfieldSet(out, 0, u8, 300)
	require.Equal(t, int64(200), old)
	require.Equal(t, int64(44), bitfieldGet(out, 0, u8))
	// #1 u4 15 → 字节 0x0f（仲裁）
	u4 := bitfieldType{signed: false, bits: 4}
	old, out = bitfieldSet(nil, 4, u4, 15)
	require.Equal(t, int64(0), old)
	require.Equal(t, []byte{0x0f}, out)
	// i8 符号解释（仲裁：存 56 读 i8 得 56；存 200 读 i8 得 -56）
	i8 := bitfieldType{signed: true, bits: 8}
	_, out = bitfieldSet(nil, 0, i8, 200)
	require.Equal(t, int64(-56), bitfieldGet(out, 0, i8))
	// 越界读零
	require.Equal(t, int64(0), bitfieldGet([]byte{0xff}, 8, u8))
}

func Test_Bitfield_when_IncrbyOverflow(t *testing.T) {
	u8 := bitfieldType{signed: false, bits: 8}
	freshU8 := func() []byte {
		_, buf := bitfieldSet(nil, 0, u8, 200)
		return buf
	}
	// WRAP 默认（仲裁：200+56 → 0）
	res, _, overflowed := bitfieldIncrby(freshU8(), 0, u8, 56, bitOverflowWrap)
	require.False(t, overflowed)
	require.Equal(t, int64(0), res)
	// SAT（仲裁：200+100 → 255）
	res, _, overflowed = bitfieldIncrby(freshU8(), 0, u8, 100, bitOverflowSat)
	require.False(t, overflowed)
	require.Equal(t, int64(255), res)
	// FAIL（仲裁：nil）
	_, _, overflowed = bitfieldIncrby(freshU8(), 0, u8, 100, bitOverflowFail)
	require.True(t, overflowed)
	// i8 回绕（仲裁：-100 + -100 → 56）
	i8 := bitfieldType{signed: true, bits: 8}
	_, buf := bitfieldSet(nil, 0, i8, -100)
	res, _, overflowed = bitfieldIncrby(buf, 0, i8, -100, bitOverflowWrap)
	require.False(t, overflowed)
	require.Equal(t, int64(56), res)
	// i64 SAT 上界（仲裁：max+1 → max）
	i64 := bitfieldType{signed: true, bits: 64}
	freshI64 := func() []byte {
		_, b := bitfieldSet(nil, 0, i64, 9223372036854775807)
		return b
	}
	res, _, overflowed = bitfieldIncrby(freshI64(), 0, i64, 1, bitOverflowSat)
	require.False(t, overflowed)
	require.Equal(t, int64(9223372036854775807), res)
	_, _, overflowed = bitfieldIncrby(freshI64(), 0, i64, 1, bitOverflowFail)
	require.True(t, overflowed)
	res, _, overflowed = bitfieldIncrby(freshI64(), 0, i64, 1, bitOverflowWrap)
	require.False(t, overflowed)
	require.Equal(t, int64(-9223372036854775808), res)
	// u1 边界
	u1 := bitfieldType{signed: false, bits: 1}
	_, buf = bitfieldSet(nil, 5, u1, 1)
	res, _, overflowed = bitfieldIncrby(buf, 5, u1, 1, bitOverflowSat)
	require.False(t, overflowed)
	require.Equal(t, int64(1), res)
}
