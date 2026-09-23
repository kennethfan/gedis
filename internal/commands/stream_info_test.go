package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func intV(n int64) protocol.Value { return protocol.Value{Kind: protocol.KindInteger, I: n} }

func Test_Stream_when_InfoStream(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	// 缺失 key / 非法子命令。
	got := dispatch(r, "XINFO", "STREAM", "missing")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "no such key")
	got = dispatch(r, "XINFO", "BOGUS", "s")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "unknown subcommand")
	got = dispatch(r, "XINFO")
	require.Equal(t, protocol.KindError, got.Kind)

	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	got = dispatch(r, "XINFO", "STREAM", "s")
	require.Equal(t, protocol.KindArray, got.Kind)
	m := arrayToMap(got.Elems)
	require.Equal(t, intV(2), m["length"])
	require.Equal(t, protocol.BulkOf("100-2"), m["last-generated-id"])
	require.Equal(t, protocol.BulkOf("0-0"), m["max-deleted-entry-id"])
	require.Equal(t, intV(2), m["entries-added"])
	require.Equal(t, protocol.BulkOf("100-1"), m["recorded-first-entry-id"])
	require.Equal(t, intV(0), m["groups"])
	require.Equal(t, entryVal("100-1", "a", "1"), m["first-entry"])
	require.Equal(t, entryVal("100-2", "a", "2"), m["last-entry"])

	// 删空后：first/last-entry 为 nil bulk，last-id 保留。
	dispatch(r, "XDEL", "s", "100-1", "100-2")
	got = dispatch(r, "XINFO", "STREAM", "s")
	m = arrayToMap(got.Elems)
	require.Equal(t, intV(0), m["length"])
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, m["first-entry"])
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, m["last-entry"])
	require.Equal(t, protocol.BulkOf("100-2"), m["last-generated-id"])
	require.Equal(t, protocol.BulkOf("0-0"), m["recorded-first-entry-id"])
}

func Test_Stream_when_InfoGroupsConsumers(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0")
	// GROUPS：entries-read nil、lag=未读数 2。
	got := dispatch(r, "XINFO", "GROUPS", "s")
	require.Equal(t, 1, len(got.Elems))
	m := arrayToMap(got.Elems[0].Elems)
	require.Equal(t, protocol.BulkOf("g"), m["name"])
	require.Equal(t, intV(0), m["consumers"])
	require.Equal(t, intV(0), m["pending"])
	require.Equal(t, protocol.BulkOf("0-0"), m["last-delivered-id"])
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, m["entries-read"])
	require.Equal(t, intV(2), m["lag"])
	// SETID 后 lag=区间内未读数。
	dispatch(r, "XGROUP", "SETID", "s", "g", "100-1")
	got = dispatch(r, "XINFO", "GROUPS", "s")
	m = arrayToMap(got.Elems[0].Elems)
	require.Equal(t, intV(1), m["lag"])
	// ENTRIESREAD 已知后 lag=added-read（可为负）。
	dispatch(r, "XGROUP", "SETID", "s", "g", "100-2", "ENTRIESREAD", "7")
	got = dispatch(r, "XINFO", "GROUPS", "s")
	m = arrayToMap(got.Elems[0].Elems)
	require.Equal(t, intV(7), m["entries-read"])
	require.Equal(t, intV(-5), m["lag"])
	// CONSUMERS：空组 → 空数组；建消费者后列出。
	got = dispatch(r, "XINFO", "CONSUMERS", "s", "g")
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}, got)
	dispatch(r, "XGROUP", "CREATECONSUMER", "s", "g", "c1")
	got = dispatch(r, "XINFO", "CONSUMERS", "s", "g")
	require.Equal(t, 1, len(got.Elems))
	m = arrayToMap(got.Elems[0].Elems)
	require.Equal(t, protocol.BulkOf("c1"), m["name"])
	require.Equal(t, intV(0), m["pending"])
	// 缺失组 → NOGROUP；缺失 key → no such key。
	got = dispatch(r, "XINFO", "CONSUMERS", "s", "nogroup")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOGROUP")
	got = dispatch(r, "XINFO", "GROUPS", "missing")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "no such key")
}

func Test_Stream_when_GroupLifecycle(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	ok := protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	// CREATE：重复 → BUSYGROUP；缺失 key → 长 ERR；MKSTREAM 建空流。
	require.Equal(t, ok, dispatch(r, "XGROUP", "CREATE", "s", "g", "0"))
	got := dispatch(r, "XGROUP", "CREATE", "s", "g", "0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "BUSYGROUP")
	got = dispatch(r, "XGROUP", "CREATE", "nosuch", "g", "0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "MKSTREAM")
	require.Equal(t, ok, dispatch(r, "XGROUP", "CREATE", "nosuch", "g", "0", "MKSTREAM"))
	require.Equal(t, intV(0), dispatch(r, "XLEN", "nosuch"))
	// 非法子命令 / 参数数。
	got = dispatch(r, "XGROUP", "BOGUS", "s", "g")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "unknown subcommand")
	got = dispatch(r, "XGROUP", "SETID", "s", "g")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "xgroup|setid")
	// SETID：缺失 key → 长 ERR；缺失组 → NOGROUP；非法 ID → Invalid。
	got = dispatch(r, "XGROUP", "SETID", "missing", "g", "0-0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "requires the key to exist")
	got = dispatch(r, "XGROUP", "SETID", "s", "nogroup", "0-0")
	require.Equal(t, protocol.KindError, got.Kind)
	got = dispatch(r, "XGROUP", "SETID", "s", "g", "bad")
	require.Equal(t, protocol.KindError, got.Kind)
	// DESTROY：1/0；缺失 key → 长 ERR（对标真 Redis）。
	require.Equal(t, intV(1), dispatch(r, "XGROUP", "DESTROY", "s", "g"))
	require.Equal(t, intV(0), dispatch(r, "XGROUP", "DESTROY", "s", "g"))
	got = dispatch(r, "XGROUP", "DESTROY", "missing", "g")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "requires the key to exist")
	// 消费者：建 1/重 0；删返 pending 数（桩恒 0）；缺组 → NOGROUP。
	dispatch(r, "XGROUP", "CREATE", "s", "g2", "0")
	require.Equal(t, intV(1), dispatch(r, "XGROUP", "CREATECONSUMER", "s", "g2", "c1"))
	require.Equal(t, intV(0), dispatch(r, "XGROUP", "CREATECONSUMER", "s", "g2", "c1"))
	require.Equal(t, intV(0), dispatch(r, "XGROUP", "DELCONSUMER", "s", "g2", "c1"))
	require.Equal(t, intV(0), dispatch(r, "XGROUP", "DELCONSUMER", "s", "g2", "c1"))
	got = dispatch(r, "XGROUP", "CREATECONSUMER", "s", "nogroup", "c1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOGROUP")
}

func Test_Stream_when_AckStub(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	RegisterStrings(r, store)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0")
	dispatch(r, "SET", "str", "v")
	// 桩：恒返 0（key/组缺失亦然），只校验 ID 合法；已存在非 stream → WRONGTYPE。
	require.Equal(t, intV(0), dispatch(r, "XACK", "s", "g", "100-1"))
	require.Equal(t, intV(0), dispatch(r, "XACK", "missing", "g", "100-1"))
	require.Equal(t, intV(0), dispatch(r, "XACK", "s", "nogroup", "100-1"))
	got := dispatch(r, "XACK", "str", "g", "100-1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGTYPE")
	got = dispatch(r, "XACK", "s", "g", "bad")
	require.Equal(t, protocol.KindError, got.Kind)
	got = dispatch(r, "XACK", "s", "g")
	require.Equal(t, protocol.KindError, got.Kind)
}

func arrayToMap(elems []protocol.Value) map[string]protocol.Value {
	m := make(map[string]protocol.Value, len(elems)/2)
	for i := 0; i+1 < len(elems); i += 2 {
		k := string(elems[i].Bulk)
		if k == "" {
			k = elems[i].S
		}
		m[k] = elems[i+1]
	}
	return m
}
