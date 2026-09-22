package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 两个 set
// When: SINTER / SUNION / SDIFF
// Then: 集合运算正确有序，缺失 key 视为空集
func Test_SetOps_when_InterUnionDiff(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SADD", "s1", "a", "b", "c")
	dispatch(r, "SADD", "s2", "b", "c", "d")
	require.Equal(t, toBulkArray([]string{"b", "c"}), dispatch(r, "SINTER", "s1", "s2"))
	require.Equal(t, toBulkArray([]string{"a", "b", "c", "d"}), dispatch(r, "SUNION", "s1", "s2"))
	require.Equal(t, toBulkArray([]string{"a"}), dispatch(r, "SDIFF", "s1", "s2"))
	require.Equal(t, toBulkArray(nil), dispatch(r, "SINTER", "s1", "missing"))
	require.Equal(t, toBulkArray([]string{"a", "b", "c"}), dispatch(r, "SUNION", "s1", "missing"))
	require.Equal(t, toBulkArray(nil), dispatch(r, "SDIFF", "missing", "s1"))
}

// Given: 两个 set
// When: SINTERSTORE / SUNIONSTORE / SDIFFSTORE
// Then: 返回结果基数，目标 key 被覆盖；空结果删除目标 key
func Test_SetOps_when_Store(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SADD", "s1", "a", "b", "c")
	dispatch(r, "SADD", "s2", "b", "c", "d")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "SINTERSTORE", "dst", "s1", "s2"))
	require.Equal(t, toBulkArray([]string{"b", "c"}), dispatch(r, "SMEMBERS", "dst"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 4}, dispatch(r, "SUNIONSTORE", "dst", "s1", "s2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SDIFFSTORE", "dst", "s1", "s2"))
	require.Equal(t, toBulkArray([]string{"a"}), dispatch(r, "SMEMBERS", "dst"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SDIFFSTORE", "dst", "missing", "s1"))
	require.Equal(t, "none", dispatch(r, "TYPE", "dst").S)
}

// Given: string 类型的 key
// When: SINTER 该 key
// Then: WRONGTYPE 错误
func Test_SetOps_when_WrongType(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SET", "s", "v")
	dispatch(r, "SADD", "set", "a")
	got := dispatch(r, "SINTER", "s", "set")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGTYPE")
	got = dispatch(r, "SUNIONSTORE", "dst", "set", "s")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGTYPE")
}

// Given: 含 4 成员的 set
// When: SSCAN 0 COUNT 2 翻页 / MATCH 过滤 / 缺失 key
// Then: 两页覆盖全部成员，MATCH 只返回匹配项
func Test_SetOps_when_Sscan(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SADD", "myset", "a", "b", "c", "d")
	page1 := dispatch(r, "SSCAN", "myset", "0", "COUNT", "2")
	require.Equal(t, protocol.KindArray, page1.Kind)
	require.Len(t, page1.Elems, 2)
	require.Equal(t, "2", string(page1.Elems[0].Bulk))
	require.Len(t, page1.Elems[1].Elems, 2)
	page2 := dispatch(r, "SSCAN", "myset", string(page1.Elems[0].Bulk))
	require.Equal(t, "0", string(page2.Elems[0].Bulk))
	require.Len(t, page2.Elems[1].Elems, 2)
	matched := dispatch(r, "SSCAN", "myset", "0", "MATCH", "a*")
	require.Len(t, matched.Elems[1].Elems, 1)
	got := dispatch(r, "SSCAN", "missing", "0")
	require.Equal(t, "0", string(got.Elems[0].Bulk))
	require.Empty(t, got.Elems[1].Elems)
}
