package datastruct

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// 真值全部来自真 Redis 7.2.6 仲裁（相同哈希+算法须逐位一致）。

func TestHLLCountArbitration(t *testing.T) {
	reg := HLLNewDense()
	require.Equal(t, uint64(0), HLLDenseCount(reg))

	require.True(t, HLLDenseAdd(reg, []byte("one")))
	require.Equal(t, uint64(1), HLLDenseCount(reg))
	require.False(t, HLLDenseAdd(reg, []byte("one")))

	for i := 0; i < 1000; i++ {
		HLLDenseAdd(reg, []byte(fmt.Sprintf("e%d", i)))
	}
	// 真值 1008；Go 与 glibc 的 sqrt/pow 末位 ulp 差异偶发 ±1（同 STOREDIST 前例），容差 1。
	require.InDelta(t, 1008, HLLDenseCount(reg), 1)
}

func TestHLLCountLarge(t *testing.T) {
	reg := HLLNewDense()
	for i := 0; i < 10000; i++ {
		HLLDenseAdd(reg, []byte(fmt.Sprintf("w%d", i)))
	}
	require.Equal(t, uint64(10073), HLLDenseCount(reg))

	mega := HLLNewDense()
	for i := 0; i < 100000; i++ {
		HLLDenseAdd(mega, []byte(fmt.Sprintf("k%d", i)))
	}
	require.Equal(t, uint64(100216), HLLDenseCount(mega))

	dst := HLLNewDense()
	HLLDenseMerge(dst, mega)
	HLLDenseMerge(dst, reg)
	require.Equal(t, uint64(110602), HLLDenseCount(dst))
}

func TestHLLMergeChanged(t *testing.T) {
	a := HLLNewDense()
	b := HLLNewDense()
	HLLDenseAdd(a, []byte("x"))
	require.True(t, HLLDenseMerge(b, a))
	require.False(t, HLLDenseMerge(b, a))
	require.Equal(t, uint64(1), HLLDenseCount(b))
}

func TestHLLEncodeRoundtrip(t *testing.T) {
	reg := HLLNewDense()
	HLLDenseAdd(reg, []byte("hello"))
	raw := EncodeHLL(reg)
	got, err := DecodeHLL(raw)
	require.NoError(t, err)
	require.Equal(t, uint64(1), HLLDenseCount(got))

	_, err = DecodeHLL([]byte("short"))
	require.Error(t, err)
	bad := EncodeHLL(reg)
	bad[0] = 'X'
	_, err = DecodeHLL(bad)
	require.Error(t, err)
}

func TestHLLKeyPrefix(t *testing.T) {
	require.Equal(t, []byte("hll:mykey"), HLLKey("mykey"))
}
