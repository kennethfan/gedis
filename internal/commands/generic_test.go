package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 各类型 key
// When: COPY 系列
// Then: 值/TTL/类型保留，REPLACE/同名/缺失语义与 Redis 一致
func Test_Generic_when_Copy(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	RegisterGeneric(r, store)
	dispatch(r, "SET", "ks", "v", "EX", "100")
	dispatch(r, "HSET", "kh", "f", "v")

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "COPY", "ks", "kd"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "kd"))
	ttl := dispatch(r, "TTL", "kd")
	require.Greater(t, ttl.I, int64(0))
	require.LessOrEqual(t, ttl.I, int64(100))

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "COPY", "ks", "kd"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "COPY", "ks", "kd", "REPLACE"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "COPY", "nosuch", "x"))
	require.Equal(t, protocol.KindError, dispatch(r, "COPY", "ks", "ks").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "COPY", "ks", "ks", "REPLACE").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "COPY", "ks", "x", "DB", "1").Kind)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "COPY", "ks", "x", "DB", "0"))

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "COPY", "kh", "kh2"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "HGET", "kh2", "f"))
	dispatch(r, "SET", "over", "s")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "COPY", "kh", "over", "REPLACE"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "HGET", "over", "f"))
}

// Given: 各类型 key
// When: RENAME/RENAMENX
// Then: 移动覆盖、TTL 保留、错误语义与 Redis 一致
func Test_Generic_when_Rename(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	RegisterGeneric(r, store)
	dispatch(r, "SET", "a", "1", "EX", "100")
	dispatch(r, "HSET", "h", "f", "v")

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "RENAME", "a", "b"))
	require.Equal(t, protocol.BulkOf("1"), dispatch(r, "GET", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: -2}, dispatch(r, "TTL", "a"))
	ttl := dispatch(r, "TTL", "b")
	require.Greater(t, ttl.I, int64(0))

	require.Equal(t, protocol.KindError, dispatch(r, "RENAME", "nosuch", "x").Kind)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "RENAME", "b", "b"))

	dispatch(r, "SET", "s1", "v")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "RENAME", "s1", "h"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "h"))

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "RENAMENX", "h", "h2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "RENAMENX", "h2", "h2"))
	dispatch(r, "SET", "busy", "x")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "RENAMENX", "h2", "busy"))
	require.Equal(t, protocol.KindError, dispatch(r, "RENAMENX", "nosuch", "x").Kind)
}

// Given: list/set/zset 数据 + 权重串
// When: SORT 全选项
// Then: 与真 Redis 仲裁值一致
func Test_Generic_when_Sort(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	RegisterList(r, store, nil)
	RegisterSet(r, store)
	RegisterZSet(r, store)
	RegisterGeneric(r, store)
	dispatch(r, "RPUSH", "mylist", "3", "1", "2")
	dispatch(r, "SADD", "myset", "b", "a", "c")
	dispatch(r, "ZADD", "sc2", "2", "10", "1", "20")
	dispatch(r, "SET", "w_a", "5")
	dispatch(r, "SET", "w_b", "3")
	dispatch(r, "SET", "w_c", "9")
	dispatch(r, "RPUSH", "items", "a", "b", "c")
	dispatch(r, "SET", "o_a", "A1")
	dispatch(r, "HSET", "uh_a", "name", "Alice")
	dispatch(r, "HSET", "uh_b", "name", "Bob")

	require.Equal(t, arr("1", "2", "3"), dispatch(r, "SORT", "mylist"))
	require.Equal(t, arr("c", "b", "a"), dispatch(r, "SORT", "myset", "ALPHA", "DESC"))
	require.Equal(t, arr("10", "20"), dispatch(r, "SORT", "sc2"))

	require.Equal(t, protocol.KindError, dispatch(r, "SORT", "myset").Kind)

	require.Equal(t, arr("b", "a", "c"), dispatch(r, "SORT", "items", "BY", "w_*"))
	require.Equal(t, arr("b", "c", "a"), dispatch(r, "SORT", "items", "BY", "o_*", "ALPHA"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "SORT", "items", "BY", "w_*", "LIMIT", "1", "2", "STORE", "out"))
	require.Equal(t, arr("a", "c"), dispatch(r, "LRANGE", "out", "0", "-1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SORT", "nosuch", "STORE", "o0"))

	got := dispatch(r, "SORT", "items", "BY", "w_*", "GET", "o_*", "GET", "w_*")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.Value{Kind: protocol.KindBulkString}, protocol.BulkOf("3"),
		protocol.BulkOf("A1"), protocol.BulkOf("5"),
		protocol.Value{Kind: protocol.KindBulkString}, protocol.BulkOf("9"),
	}}, got)

	got = dispatch(r, "SORT", "items", "BY", "w_*", "GET", "uh_*->name")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("Bob"), protocol.BulkOf("Alice"), protocol.Value{Kind: protocol.KindBulkString},
	}}, got)

	require.Equal(t, arr("a", "b", "c"), dispatch(r, "SORT", "items", "ALPHA", "GET", "#"))
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}, dispatch(r, "SORT", "nosuch"))
	dispatch(r, "SET", "ks", "v")
	require.Equal(t, protocol.KindError, dispatch(r, "SORT", "ks").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "SORT", "items", "STORE").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "SORT", "items", "LIMIT", "x", "y").Kind)
}

func arr(elems ...string) protocol.Value {
	out := make([]protocol.Value, 0, len(elems))
	for _, e := range elems {
		out = append(out, protocol.BulkOf(e))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}
