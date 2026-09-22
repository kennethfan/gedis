package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 列表 [a b]
// When: BLPOP mylist 1 / BRPOP mylist 1
// Then: 立即返回 [mylist a] / [mylist b]
func Test_List_when_BlockingHit(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b")
	got := dispatch(r, "BLPOP", "mylist", "1")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("mylist"), got.Elems[0])
	require.Equal(t, protocol.BulkOf("a"), got.Elems[1])
	got = dispatch(r, "BRPOP", "mylist", "1")
	require.Equal(t, protocol.BulkOf("b"), got.Elems[1])
}

// Given: 空库
// When: BLPOP nope 0.1（多个 key，全部缺失）
// Then: 超时后返回 null array
func Test_List_when_BlockingTimeout(t *testing.T) {
	r, _ := openListSetup(t)
	got := dispatch(r, "BLPOP", "k1", "k2", "0.1")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Nil(t, got.Elems)
}

// Given: 列表 [a b c]
// When: BLMPOP 1 1 mylist LEFT COUNT 2 / BRMPOP 超时
// Then: [mylist [a b]] / 超时 null
func Test_List_when_Blmpop(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b", "c")
	got := dispatch(r, "BLMPOP", "1", "1", "mylist", "LEFT", "COUNT", "2")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("mylist"), got.Elems[0])
	require.Equal(t, bulkList("a", "b"), got.Elems[1])
	got = dispatch(r, "BRMPOP", "0.1", "1", "nope", "RIGHT")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Nil(t, got.Elems)
}

func intList(ns ...int64) protocol.Value {
	out := make([]protocol.Value, len(ns))
	for i, n := range ns {
		out[i] = protocol.Value{Kind: protocol.KindInteger, I: n}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

// When: LPOS mylist a / RANK 2 / COUNT 0 / MAXLEN 3 / 不存在元素
// Then: 0 / 2 / [0 2 4] / [0 2] / null
func Test_List_when_Lpos(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b", "a", "c", "a")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "LPOS", "mylist", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "LPOS", "mylist", "a", "RANK", "2"))
	require.Equal(t, intList(0, 2, 4), dispatch(r, "LPOS", "mylist", "a", "COUNT", "0"))
	require.Equal(t, intList(0, 2), dispatch(r, "LPOS", "mylist", "a", "COUNT", "0", "MAXLEN", "3"))
	got := dispatch(r, "LPOS", "mylist", "zzz")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}
