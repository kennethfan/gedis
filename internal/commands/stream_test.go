package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func bulk(ss ...string) []protocol.Value {
	out := make([]protocol.Value, len(ss))
	for i, s := range ss {
		out[i] = protocol.BulkOf(s)
	}
	return out
}

func entryVal(id string, fields ...string) protocol.Value {
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(id),
		protocol.Value{Kind: protocol.KindArray, Elems: bulk(fields...)},
	}}
}

func Test_Stream_when_AddRangeLen(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	require.Equal(t, protocol.BulkOf("100-1"),
		dispatch(r, "XADD", "s", "100-1", "a", "1"))
	require.Equal(t, protocol.BulkOf("100-2"),
		dispatch(r, "XADD", "s", "100-2", "a", "2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "XLEN", "s"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XLEN", "missing"))
	got := dispatch(r, "XRANGE", "s", "-", "+")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		entryVal("100-1", "a", "1"),
		entryVal("100-2", "a", "2"),
	}}, got)
	got = dispatch(r, "XREVRANGE", "s", "+", "-")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		entryVal("100-2", "a", "2"),
		entryVal("100-1", "a", "1"),
	}}, got)
	got = dispatch(r, "XRANGE", "s", "-", "+", "COUNT", "1")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		entryVal("100-1", "a", "1"),
	}}, got)
	// 缺失 key → 空数组；逆序区间 → 空数组。
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}},
		dispatch(r, "XRANGE", "missing", "-", "+"))
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}},
		dispatch(r, "XRANGE", "s", "+", "-"))
}

func Test_Stream_when_AddErrors(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	// 非法 ID。
	got := dispatch(r, "XADD", "s", "bad", "a", "1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Invalid stream ID")
	// 0-0。
	got = dispatch(r, "XADD", "s", "0-0", "a", "1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "greater than 0-0")
	// 奇数个 field。
	got = dispatch(r, "XADD", "s", "*", "onlyfield")
	require.Equal(t, protocol.KindError, got.Kind)
	// 小于等于 top。
	dispatch(r, "XADD", "s", "100-5", "a", "1")
	got = dispatch(r, "XADD", "s", "100-5", "a", "2")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "equal or smaller")
}

func Test_Stream_when_Del(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XDEL", "s", "100-1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XDEL", "s", "100-9"))
	// 非法 ID 整个命令报错。
	got := dispatch(r, "XDEL", "s", "bad-id")
	require.Equal(t, protocol.KindError, got.Kind)
	// 删光后 key 保留。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XDEL", "s", "100-2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XLEN", "s"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XDEL", "missing", "100-1"))
}

func Test_Stream_when_WrongType(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	RegisterStrings(r, store)
	dispatch(r, "SET", "str", "v")
	for _, args := range [][]string{
		{"XADD", "str", "*", "a", "1"},
		{"XLEN", "str"},
		{"XRANGE", "str", "-", "+"},
		{"XREVRANGE", "str", "+", "-"},
		{"XDEL", "str", "1-1"},
	} {
		got := dispatch(r, args...)
		require.Equal(t, protocol.KindError, got.Kind, args)
		require.Contains(t, got.S, "WRONGTYPE", args)
	}
}
