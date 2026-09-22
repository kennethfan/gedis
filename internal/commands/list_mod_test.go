package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 列表 [a b c]
// When: LSET mylist 1 x
// Then: +OK，索引 1 变为 x
func Test_List_when_Lset(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b", "c")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "LSET", "mylist", "1", "x"))
	require.Equal(t, protocol.BulkOf("x"), dispatch(r, "LINDEX", "mylist", "1"))
}

// Given: 列表 [a]
// When: LSET mylist 5 x / LSET 缺失 key
// Then: index out of range / no such key 错误
func Test_List_when_LsetOutOfRange(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a")
	got := dispatch(r, "LSET", "mylist", "5", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "index out of range")
	got = dispatch(r, "LSET", "nope", "0", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "no such key")
}

// Given: 列表 [a b c]
// When: LINSERT mylist BEFORE b x / AFTER c y / 找不到 pivot
// Then: 新长度 4/5，顺序正确；pivot 不存在返回 -1
func Test_List_when_Linsert(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b", "c")
	got := dispatch(r, "LINSERT", "mylist", "BEFORE", "b", "x")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 4}, got)
	got = dispatch(r, "LINSERT", "mylist", "AFTER", "c", "y")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 5}, got)
	require.Equal(t, bulkList("a", "x", "b", "c", "y"), dispatch(r, "LRANGE", "mylist", "0", "-1"))
	got = dispatch(r, "LINSERT", "mylist", "BEFORE", "zzz", "q")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: -1}, got)
}

// Given: 列表 [a b a c a]
// When: LREM mylist 2 a / LREM mylist -1 a / LREM mylist 0 b
// Then: 分别删除 2/1/1 个，剩余顺序正确
func Test_List_when_Lrem(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b", "a", "c", "a")
	got := dispatch(r, "LREM", "mylist", "2", "a")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, got)
	require.Equal(t, bulkList("b", "c", "a"), dispatch(r, "LRANGE", "mylist", "0", "-1"))
	got = dispatch(r, "LREM", "mylist", "-1", "a")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, got)
	require.Equal(t, bulkList("b", "c"), dispatch(r, "LRANGE", "mylist", "0", "-1"))
	got = dispatch(r, "LREM", "mylist", "0", "b")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, got)
	require.Equal(t, bulkList("c"), dispatch(r, "LRANGE", "mylist", "0", "-1"))
}

// Given: 列表 [a b c d]
// When: LTRIM mylist 1 2 / LTRIM 缺失 key
// Then: 只剩 [b c]；缺失 key 返回 +OK
func Test_List_when_Ltrim(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b", "c", "d")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "LTRIM", "mylist", "1", "2"))
	require.Equal(t, bulkList("b", "c"), dispatch(r, "LRANGE", "mylist", "0", "-1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "LTRIM", "nope", "0", "-1"))
}
