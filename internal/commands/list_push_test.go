package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openListSetup(t testing.TB) (*network.Router, *storage.Pebble) {
	t.Helper()
	r, store := openTestSetup(t)
	RegisterList(r, store, nil)
	return r, store
}

func bulkList(elems ...string) protocol.Value {
	out := make([]protocol.Value, len(elems))
	for i, e := range elems {
		out[i] = protocol.BulkOf(e)
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

// Given: 空库
// When: RPUSH mylist a b c
// Then: 返回 3，LRANGE 全量有序
func Test_List_when_RpushLrange(t *testing.T) {
	r, _ := openListSetup(t)
	got := dispatch(r, "RPUSH", "mylist", "a", "b", "c")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, got)
	require.Equal(t, bulkList("a", "b", "c"), dispatch(r, "LRANGE", "mylist", "0", "-1"))
}

// Given: 已有列表 [a b c]
// When: LPUSH mylist x
// Then: 返回 4，表头为 x
func Test_List_when_Lpush(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b", "c")
	got := dispatch(r, "LPUSH", "mylist", "x")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 4}, got)
	require.Equal(t, bulkList("x", "a", "b", "c"), dispatch(r, "LRANGE", "mylist", "0", "-1"))
}

// Given: 列表 [a b c]
// When: LPOP / RPOP / LPOP count 2
// Then: 返回 a / c / [b]，空时弹 null
func Test_List_when_Pop(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b", "c")
	require.Equal(t, protocol.BulkOf("a"), dispatch(r, "LPOP", "mylist"))
	require.Equal(t, protocol.BulkOf("c"), dispatch(r, "RPOP", "mylist"))
	require.Equal(t, bulkList("b"), dispatch(r, "LPOP", "mylist", "2"))
	got := dispatch(r, "LPOP", "mylist")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: 列表 [a b c]
// When: LLEN / LINDEX 0 / LINDEX -1 / LINDEX 越界
// Then: 3 / a / c / null
func Test_List_when_LenIndex(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b", "c")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "LLEN", "mylist"))
	require.Equal(t, protocol.BulkOf("a"), dispatch(r, "LINDEX", "mylist", "0"))
	require.Equal(t, protocol.BulkOf("c"), dispatch(r, "LINDEX", "mylist", "-1"))
	got := dispatch(r, "LINDEX", "mylist", "9")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: 缺失 key
// When: LLEN / LRANGE
// Then: 0 / 空数组（非 null）
func Test_List_when_Missing(t *testing.T) {
	r, _ := openListSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "LLEN", "nope"))
	require.Equal(t, bulkList(), dispatch(r, "LRANGE", "nope", "0", "-1"))
}

// Given: string key
// When: LPUSH
// Then: WRONGTYPE 错误
func Test_List_when_WrongType(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "SET", "str", "v")
	got := dispatch(r, "LPUSH", "str", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGTYPE")
}
