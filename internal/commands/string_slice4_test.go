package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: key 不存在
// When: APPEND key hello
// Then: 返回 5，GET 得 hello
func Test_Append_WhenMissing_ThenCreate(t *testing.T) {
	r, _ := openTestSetup(t)
	got := dispatch(r, "APPEND", "k", "hello")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 5}, got)
	require.Equal(t, protocol.BulkOf("hello"), dispatch(r, "GET", "k"))
}

// Given: k=hello
// When: APPEND k " world"
// Then: 返回 11
func Test_Append_WhenExists_ThenLength(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "hello")
	got := dispatch(r, "APPEND", "k", " world")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 11}, got)
}

// Given: k=hello；missing 不存在
// When: STRLEN k / STRLEN missing
// Then: 5 / 0
func Test_StrLen_WhenExistsAndMissing(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "hello")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 5}, dispatch(r, "STRLEN", "k"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "STRLEN", "missing"))
}

// Given: k="hello world"
// When: SETRANGE k 6 Redis
// Then: 返回 11，GET 得 "hello Redis"
func Test_SetRange_WhenInside_ThenReplace(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "hello world")
	got := dispatch(r, "SETRANGE", "k", "6", "Redis")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 11}, got)
	require.Equal(t, protocol.BulkOf("hello Redis"), dispatch(r, "GET", "k"))
}

// Given: k=hi
// When: SETRANGE k 5 x
// Then: 返回 6，中间补零
func Test_SetRange_WhenBeyond_ThenZeroPad(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "hi")
	got := dispatch(r, "SETRANGE", "k", "5", "x")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 6}, got)
	require.Equal(t, protocol.BulkOf("hi\x00\x00\x00x"), dispatch(r, "GET", "k"))
}

// Given: k="hello world"
// When: GETRANGE k 0 3 / GETRANGE k -5 -1 / GETRANGE k 20 30
// Then: hell / world / 空串
func Test_GetRange_WhenRanges_ThenSlices(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "hello world")
	require.Equal(t, protocol.BulkOf("hell"), dispatch(r, "GETRANGE", "k", "0", "3"))
	require.Equal(t, protocol.BulkOf("world"), dispatch(r, "GETRANGE", "k", "-5", "-1"))
	require.Equal(t, protocol.BulkOf(""), dispatch(r, "GETRANGE", "k", "20", "30"))
}

// Given: k=v 存在；missing 不存在
// When: TYPE k / TYPE missing
// Then: string / none
func Test_Type_WhenStringAndMissing(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "string"}, dispatch(r, "TYPE", "k"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "none"}, dispatch(r, "TYPE", "missing"))
}

// Given: k1 k2 存在
// When: EXISTS k1 k2 missing
// Then: 2
func Test_Exists_WhenMixed_ThenCount(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k1", "v1")
	dispatch(r, "SET", "k2", "v2")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "EXISTS", "k1", "k2", "missing"))
}

// Given: user:1 user:2 other 存在
// When: KEYS user:*
// Then: 返回 2 个且不含存储前缀
func Test_Keys_WhenPattern_ThenMatched(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "user:1", "a")
	dispatch(r, "SET", "user:2", "b")
	dispatch(r, "SET", "other", "c")
	got := dispatch(r, "KEYS", "user:*")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	names := []string{string(got.Elems[0].Bulk), string(got.Elems[1].Bulk)}
	require.ElementsMatch(t, []string{"user:1", "user:2"}, names)
}
