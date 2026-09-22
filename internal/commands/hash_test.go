package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openHashSetup(t testing.TB) (*network.Router, *storage.Pebble) {
	t.Helper()
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	return r, store
}

// Given: 空库
// When: HSET h f v / HGET h f
// Then: HSET 返回新增数 1，HGET 返回 bulk v
func Test_Hash_when_SetGet(t *testing.T) {
	r, _ := openHashSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "HSET", "h", "f", "v"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "HGET", "h", "f"))
}

// Given: 已存在的 field
// When: HSET 同 field 新值 / HGET 不存在的 field
// Then: 返回 0（更新），缺失 field 返回 null
func Test_Hash_when_UpdateAndMissing(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "f", "v")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "HSET", "h", "f", "new"))
	require.Equal(t, protocol.BulkOf("new"), dispatch(r, "HGET", "h", "f"))
	got := dispatch(r, "HGET", "h", "nope")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
	got = dispatch(r, "HGET", "missing", "f")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: 多 field hash
// When: HDEL 部分 field / HLEN / HEXISTS
// Then: 删除计数正确，长度与存在性正确
func Test_Hash_when_DelLenExists(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "HDEL", "h", "f1", "f2", "nope"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "HLEN", "h"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "HEXISTS", "h", "f3"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "HEXISTS", "h", "f1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "HLEN", "missing"))
}

// Given: 空库与已存在 field
// When: HSETNX 新 field / HSETNX 已存在 field
// Then: 新建返回 1，既有返回 0 且值不变
func Test_Hash_when_Setnx(t *testing.T) {
	r, _ := openHashSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "HSETNX", "h", "f", "v"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "HSETNX", "h", "f", "new"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "HGET", "h", "f"))
}

// Given: string 类型的 key
// When: HGET 该 key
// Then: WRONGTYPE 错误
func Test_Hash_when_WrongType(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "SET", "s", "v")
	got := dispatch(r, "HGET", "s", "f")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGTYPE")
}

// Given: 空库
// When: HMSET 多 field / HMGET 含缺失 field
// Then: +OK，缺失 field 返回 null
func Test_Hash_when_HmsetHmget(t *testing.T) {
	r, _ := openHashSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "HMSET", "h", "f1", "v1", "f2", "v2"))
	got := dispatch(r, "HMGET", "h", "f1", "nope", "f2")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 3)
	require.Equal(t, protocol.BulkOf("v1"), got.Elems[0])
	require.Equal(t, protocol.KindBulkString, got.Elems[1].Kind)
	require.Nil(t, got.Elems[1].Bulk)
	require.Equal(t, protocol.BulkOf("v2"), got.Elems[2])
}

// Given: 含 2 field 的 hash
// When: HGETALL / HKEYS / HVALS
// Then: 有序返回 field-value 对、keys、values；缺失 key 返回空数组
func Test_Hash_when_GetallKeysVals(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "b", "2", "a", "1")
	got := dispatch(r, "HGETALL", "h")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 4)
	require.Equal(t, protocol.BulkOf("a"), got.Elems[0])
	require.Equal(t, protocol.BulkOf("1"), got.Elems[1])
	require.Equal(t, protocol.BulkOf("b"), got.Elems[2])
	require.Equal(t, protocol.BulkOf("2"), got.Elems[3])
	got = dispatch(r, "HKEYS", "h")
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("a"), got.Elems[0])
	got = dispatch(r, "HVALS", "h")
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("1"), got.Elems[0])
	got = dispatch(r, "HGETALL", "missing")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Empty(t, got.Elems)
}

// Given: 含 4 field 的 hash
// When: HSCAN 0 COUNT 2 / HSCAN 返回 cursor 翻页 / MATCH 过滤
// Then: 两页覆盖全部 field，MATCH 只返回匹配项
func Test_Hash_when_Hscan(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "a", "1", "b", "2", "c", "3", "d", "4")
	page1 := dispatch(r, "HSCAN", "h", "0", "COUNT", "2")
	require.Equal(t, protocol.KindArray, page1.Kind)
	require.Len(t, page1.Elems, 2)
	require.Equal(t, "2", string(page1.Elems[0].Bulk))
	require.Len(t, page1.Elems[1].Elems, 4)
	page2 := dispatch(r, "HSCAN", "h", string(page1.Elems[0].Bulk))
	require.Equal(t, "0", string(page2.Elems[0].Bulk))
	require.Len(t, page2.Elems[1].Elems, 4)
	matched := dispatch(r, "HSCAN", "h", "0", "MATCH", "a*")
	require.Len(t, matched.Elems[1].Elems, 2)
}

// Given: hash field 存整数
// When: HINCRBY / HINCRBY 不存在 field / 非整数 field
// Then: 增量正确，缺失 field 从 0 起算，非整数报错
func Test_Hash_when_Hincrby(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "n", "10", "s", "xx")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 15}, dispatch(r, "HINCRBY", "h", "n", "5"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "HINCRBY", "h", "new", "3"))
	got := dispatch(r, "HINCRBY", "h", "s", "1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "not an integer")
}

// Given: hash field 存浮点数
// When: HINCRBYFLOAT / 非浮点 field
// Then: 最短往返表示，非浮点报错
func Test_Hash_when_Hincrbyfloat(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "f", "10.5", "s", "xx")
	got := dispatch(r, "HINCRBYFLOAT", "h", "f", "0.1")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Equal(t, "10.6", string(got.Bulk))
	got = dispatch(r, "HINCRBYFLOAT", "h", "s", "0.1")
	require.Equal(t, protocol.KindError, got.Kind)
}

// Given: hash key
// When: EXPIRE / TTL / PERSIST
// Then: 过期语义与 string key 一致
func Test_Hash_when_ExpireTTL(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "f", "v")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "EXPIRE", "h", "100"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 100}, dispatch(r, "TTL", "h"))
	require.Equal(t, "hash", dispatch(r, "TYPE", "h").S)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "PERSIST", "h"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: -1}, dispatch(r, "TTL", "h"))
}

// Given: 小 hash 与大 hash
// When: OBJECT ENCODING
// Then: 小 hash 返回 listpack，大 hash 返回 hashtable
func Test_Hash_when_ObjectEncoding(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "small", "f", "v")
	require.Equal(t, "listpack", string(dispatch(r, "OBJECT", "ENCODING", "small").Bulk))
	dispatch(r, "HSET", "big", "f", string(make([]byte, 65)))
	got := dispatch(r, "OBJECT", "ENCODING", "big")
	require.Equal(t, "hashtable", string(got.Bulk))
}

// Given: string key 与 hash key 并存
// When: KEYS *
// Then: 两类 key 都返回
func Test_Hash_when_KeysCrossType(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "SET", "str", "v")
	dispatch(r, "HSET", "hsh", "f", "v")
	got := dispatch(r, "KEYS", "*")
	require.Equal(t, protocol.KindArray, got.Kind)
	names := map[string]bool{}
	for _, e := range got.Elems {
		names[string(e.Bulk)] = true
	}
	require.True(t, names["str"])
	require.True(t, names["hsh"])
}
