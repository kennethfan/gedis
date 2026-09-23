package commands

import (
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func streamKeyVal(key string, entries ...protocol.Value) protocol.Value {
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(key),
		protocol.Value{Kind: protocol.KindArray, Elems: entries},
	}}
}

func Test_Stream_when_ReadBasic(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "k1", "100-1", "a", "1")
	dispatch(r, "XADD", "k1", "100-2", "a", "2")
	dispatch(r, "XADD", "k2", "200-1", "b", "1")
	got := dispatch(r, "XREAD", "STREAMS", "k1", "k2", "0-0", "0-0")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		streamKeyVal("k1", entryVal("100-1", "a", "1"), entryVal("100-2", "a", "2")),
		streamKeyVal("k2", entryVal("200-1", "b", "1")),
	}}, got)
	// $ 取不到历史；COUNT 按流限数；显式 ID 过滤。
	require.Equal(t, protocol.Value{Kind: protocol.KindArray},
		dispatch(r, "XREAD", "STREAMS", "k1", "$"))
	got = dispatch(r, "XREAD", "COUNT", "1", "STREAMS", "k1", "k2", "0-0", "0-0")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		streamKeyVal("k1", entryVal("100-1", "a", "1")),
		streamKeyVal("k2", entryVal("200-1", "b", "1")),
	}}, got)
	got = dispatch(r, "XREAD", "STREAMS", "k1", "100-1")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		streamKeyVal("k1", entryVal("100-2", "a", "2")),
	}}, got)
	// 缺失 key 跳过；全缺失 → nil 数组。
	got = dispatch(r, "XREAD", "STREAMS", "k1", "missing", "0-0", "0-0")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		streamKeyVal("k1", entryVal("100-1", "a", "1"), entryVal("100-2", "a", "2")),
	}}, got)
	require.Equal(t, protocol.Value{Kind: protocol.KindArray},
		dispatch(r, "XREAD", "STREAMS", "missing", "0-0"))
}

func Test_Stream_when_ReadErrors(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	RegisterStrings(r, store)
	dispatch(r, "SET", "str", "v")
	got := dispatch(r, "XREAD", "STREAMS", "k1", ">")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "can be specified only when calling XREADGROUP")
	got = dispatch(r, "XREAD", "COUNT", "1", "k", "0-0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "syntax error")
	got = dispatch(r, "XREAD", "STREAMS", "k")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments")
	got = dispatch(r, "XREAD", "STREAMS", "k1", "k2", "0-0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Unbalanced")
	got = dispatch(r, "XREAD", "STREAMS", "k1", "bad")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Invalid stream ID")
	got = dispatch(r, "XREAD", "BLOCK", "x", "STREAMS", "k1", "0-0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "not an integer or out of range")
	got = dispatch(r, "XREAD", "BLOCK", "-1", "STREAMS", "k1", "0-0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "timeout is negative")
	got = dispatch(r, "XREAD", "STREAMS", "str", "0-0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGTYPE")
}

func Test_Stream_when_ReadBlock(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "k", "100-1", "a", "1")
	// 超时无数据 → nil。
	got := dispatch(r, "XREAD", "BLOCK", "100", "STREAMS", "k", "$")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray}, got)
	// 后台写入唤醒等待者。
	done := make(chan protocol.Value, 1)
	go func() {
		done <- dispatch(r, "XREAD", "BLOCK", "5000", "STREAMS", "k", "$")
	}()
	time.Sleep(100 * time.Millisecond)
	newID := dispatch(r, "XADD", "k", "*", "a", "2")
	require.Equal(t, protocol.KindBulkString, newID.Kind)
	got = <-done
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		streamKeyVal("k", entryVal(string(newID.Bulk), "a", "2")),
	}}, got)
}
