package commands

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

// fp32Bulk 造小端 float32 二进制 bulk（VADD/VSIM FP32 形态用）。
func fp32Bulk(v ...float32) string {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return string(b)
}

func openVectorSetup(t *testing.T) (*network.Router, *storage.Pebble) {
	t.Helper()
	r, store := openTestSetup(t)
	RegisterVector(r, store)
	return r, store
}

// Given: 空库
// When: VALUES 形态 VADD/VCARD/VDIM/VEMB/VREM 全链路
// Then: 新增回 1、覆盖回 0、维度/基数/向量/删除语义正确
func Test_Vector_when_ValuesRoundTrip(t *testing.T) {
	r, _ := openVectorSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "VADD", "k", "VALUES", "3", "1 0 0", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "VADD", "k", "VALUES", "3", "0 1 0", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "VCARD", "k"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3},
		dispatch(r, "VDIM", "k"))
	require.Equal(t, toBulkArray([]string{"0", "1", "0"}),
		dispatch(r, "VEMB", "k", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString},
		dispatch(r, "VEMB", "k", "nope"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "VREM", "k", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "VREM", "k", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "VCARD", "k"))
}

// Given: 空库
// When: FP32 形态 VADD + 缺键/错键/维度错/选项错
// Then: 二进制向量正确落盘，错误文案对齐
func Test_Vector_when_FP32AndErrors(t *testing.T) {
	r, store := openVectorSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "VADD", "f", "FP32", fp32Bulk(1, 2), "p"))
	require.Equal(t, toBulkArray([]string{"1", "2"}),
		dispatch(r, "VEMB", "f", "p"))
	// 缺键读回零值。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "VCARD", "missing"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "VDIM", "missing"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString},
		dispatch(r, "VRANDMEMBER", "missing"))
	require.Equal(t, toBulkArray(nil),
		dispatch(r, "VRANDMEMBER", "missing", "3"))
	require.Equal(t, toBulkArray(nil),
		dispatch(r, "VSIM", "missing", "VALUES", "2", "1 0"))
	// 维度不一致。
	require.Equal(t, protocol.KindError,
		dispatch(r, "VADD", "f", "VALUES", "3", "1 2 3", "q").Kind)
	// 不支持的选项一律 syntax error。
	require.Equal(t, protocol.KindError,
		dispatch(r, "VADD", "f", "VALUES", "2", "1 2", "q", "Q8").Kind)
	require.Equal(t, protocol.KindError,
		dispatch(r, "VADD", "f", "1 2", "q").Kind)
	// arity。
	require.Equal(t, protocol.KindError, dispatch(r, "VADD", "f").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "VCARD").Kind)
	// WRONGTYPE。
	RegisterStrings(r, store)
	dispatch(r, "SET", "s", "v")
	require.Equal(t, "WRONGTYPE Key is not a valid vectorset value.",
		dispatch(r, "VCARD", "s").S)
}

// Given: a=[1,0] b=[0,1]
// When: VSIM 三形态 + WITHSCORES/COUNT
// Then: 余弦排序精确，分数 1/0
func Test_Vector_when_SimSearch(t *testing.T) {
	r, _ := openVectorSetup(t)
	dispatch(r, "VADD", "k", "VALUES", "2", "1 0", "a")
	dispatch(r, "VADD", "k", "VALUES", "2", "0 1", "b")
	require.Equal(t, toBulkArray([]string{"a", "b"}),
		dispatch(r, "VSIM", "k", "VALUES", "2", "1 0"))
	require.Equal(t, toBulkArray([]string{"a", "1", "b", "0"}),
		dispatch(r, "VSIM", "k", "VALUES", "2", "1 0", "WITHSCORES"))
	require.Equal(t, toBulkArray([]string{"a"}),
		dispatch(r, "VSIM", "k", "VALUES", "2", "1 0", "COUNT", "1"))
	require.Equal(t, toBulkArray([]string{"b", "a"}),
		dispatch(r, "VSIM", "k", "ELE", "b"))
	require.Equal(t, toBulkArray([]string{"a", "b"}),
		dispatch(r, "VSIM", "k", "FP32", fp32Bulk(1, 0)))
	require.Equal(t, protocol.KindError,
		dispatch(r, "VSIM", "k", "ELE", "nope").Kind)
	require.Equal(t, protocol.KindError,
		dispatch(r, "VSIM", "k", "VALUES", "2", "1 0", "FILTER", "x").Kind)
}

// Given: 3 元素集
// When: VRANDMEMBER 单个/count 正负
// Then: 语义仿 SRANDMEMBER（正 clamp、负可重复）
func Test_Vector_when_RandMember(t *testing.T) {
	r, _ := openVectorSetup(t)
	dispatch(r, "VADD", "k", "VALUES", "1", "1", "a")
	dispatch(r, "VADD", "k", "VALUES", "1", "2", "b")
	dispatch(r, "VADD", "k", "VALUES", "1", "3", "c")
	single := dispatch(r, "VRANDMEMBER", "k")
	require.Equal(t, protocol.KindBulkString, single.Kind)
	require.Contains(t, []string{"a", "b", "c"}, string(single.Bulk))
	got := dispatch(r, "VRANDMEMBER", "k", "9")
	require.Equal(t, 3, len(got.Elems))
	got = dispatch(r, "VRANDMEMBER", "k", "-5")
	require.Equal(t, 5, len(got.Elems))
}
