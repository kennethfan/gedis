package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 空库
// When: MSET k1 v1 k2 v2
// Then: 返回 OK, GET k1 得 v1
func Test_MSet_WhenPairs_ThenOk(t *testing.T) {
	r, _ := openTestSetup(t)
	got := dispatch(r, "MSET", "k1", "v1", "k2", "v2")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, got)
	require.Equal(t, protocol.BulkOf("v1"), dispatch(r, "GET", "k1"))
	require.Equal(t, protocol.BulkOf("v2"), dispatch(r, "GET", "k2"))
}

// Given: 空库
// When: MSET 参数个数为奇数
// Then: 返回 arity 错误
func Test_MSet_WhenOddArgs_ThenArityError(t *testing.T) {
	r, _ := openTestSetup(t)
	got := dispatch(r, "MSET", "k1")
	require.Equal(t, protocol.KindError, got.Kind)
}

// Given: k1=v1 已存在
// When: MGET k1 missing
// Then: 返回 [v1, nil]
func Test_MGet_WhenMixed_ThenArrayWithNil(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k1", "v1")
	got := dispatch(r, "MGET", "k1", "missing")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("v1"), got.Elems[0])
	require.Equal(t, protocol.Value{Kind: protocol.KindNull}, got.Elems[1])
}

// Given: 空库
// When: MSETNX k1 v1 k2 v2
// Then: 返回 1, 键已写入
func Test_MSetNX_WhenAllNew_ThenOne(t *testing.T) {
	r, _ := openTestSetup(t)
	got := dispatch(r, "MSETNX", "k1", "v1", "k2", "v2")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, got)
	require.Equal(t, protocol.BulkOf("v1"), dispatch(r, "GET", "k1"))
}

// Given: k1 已存在
// When: MSETNX k1 vX k2 v2
// Then: 返回 0, k1 与 k2 均未被修改
func Test_MSetNX_WhenOneExists_ThenZeroAndUntouched(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k1", "v1")
	got := dispatch(r, "MSETNX", "k1", "vX", "k2", "v2")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, got)
	require.Equal(t, protocol.BulkOf("v1"), dispatch(r, "GET", "k1"))
	got = dispatch(r, "GET", "k2")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: key 不存在
// When: INCR key
// Then: 返回 1
func Test_Incr_WhenMissing_ThenOne(t *testing.T) {
	r, _ := openTestSetup(t)
	got := dispatch(r, "INCR", "n")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, got)
}

// Given: n=10
// When: INCR n / INCRBY n 5 / DECR n / DECRBY n 3
// Then: 依次返回 11 / 16 / 15 / 12
func Test_IncrFamily_WhenIntegers_ThenSequence(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "n", "10")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 11}, dispatch(r, "INCR", "n"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 16}, dispatch(r, "INCRBY", "n", "5"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 15}, dispatch(r, "DECR", "n"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 12}, dispatch(r, "DECRBY", "n", "3"))
}

// Given: s=abc
// When: INCR s
// Then: 返回非整数错误
func Test_Incr_WhenNotInteger_ThenError(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "s", "abc")
	got := dispatch(r, "INCR", "s")
	require.Equal(t, protocol.KindError, got.Kind)
}

// Given: f=10.5
// When: INCRBYFLOAT f 0.1
// Then: 返回 10.6
func Test_IncrByFloat_WhenValid_ThenSum(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "f", "10.5")
	got := dispatch(r, "INCRBYFLOAT", "f", "0.1")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Equal(t, "10.6", string(got.Bulk))
}
