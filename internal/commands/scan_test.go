package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 六种类型各一 key
// When: SCAN 全量 / MATCH / COUNT 分页 / TYPE 过滤
// Then: 全集正确，分页走完不丢不重
func Test_Scan_when_GlobalMatchType(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	RegisterList(r, store, nil)
	RegisterSet(r, store)
	RegisterZSet(r, store)
	RegisterGeo(r, store)
	RegisterScan(r, store)
	dispatch(r, "SET", "str1", "v")
	dispatch(r, "HSET", "h1", "f", "v")
	dispatch(r, "RPUSH", "l1", "e")
	dispatch(r, "SADD", "s1", "m")
	dispatch(r, "ZADD", "z1", "1", "m")
	dispatch(r, "GEOADD", "g1", "10", "10", "p")

	got := dispatch(r, "SCAN", "0")
	require.Equal(t, protocol.BulkOf("0"), got.Elems[0])
	require.Len(t, got.Elems[1].Elems, 6)

	got = dispatch(r, "SCAN", "0", "MATCH", "s*")
	names := bulkSet(got.Elems[1].Elems)
	require.Equal(t, map[string]bool{"str1": true, "s1": true}, names)
	require.Equal(t, protocol.BulkOf("0"), got.Elems[0])

	got = dispatch(r, "SCAN", "0", "TYPE", "zset")
	names = bulkSet(got.Elems[1].Elems)
	require.Equal(t, map[string]bool{"z1": true, "g1": true}, names)

	got = dispatch(r, "SCAN", "0", "TYPE", "hash")
	require.Len(t, got.Elems[1].Elems, 1)

	got = dispatch(r, "SCAN", "0", "TYPE", "nope")
	require.Len(t, got.Elems[1].Elems, 0)
	require.Equal(t, protocol.BulkOf("0"), got.Elems[0])

	seen := map[string]bool{}
	cursor := "0"
	for {
		page := dispatch(r, "SCAN", cursor, "COUNT", "2")
		for _, e := range page.Elems[1].Elems {
			seen[string(e.Bulk)] = true
		}
		cursor = string(page.Elems[0].Bulk)
		if cursor == "0" {
			break
		}
	}
	require.Len(t, seen, 6)
	require.Equal(t, protocol.KindError, dispatch(r, "SCAN", "xx").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "SCAN").Kind)
}

func bulkSet(elems []protocol.Value) map[string]bool {
	out := make(map[string]bool, len(elems))
	for _, e := range elems {
		out[string(e.Bulk)] = true
	}
	return out
}

// Given: 含 field 的 hash
// When: HSCAN NOVALUES
// Then: 只回 field；旧行为不变
func Test_Scan_when_HscanNovalues(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	RegisterScan(r, store)
	dispatch(r, "HSET", "h", "f1", "v1", "f2", "v2")
	got := dispatch(r, "HSCAN", "h", "0", "NOVALUES")
	require.Equal(t, protocol.BulkOf("0"), got.Elems[0])
	require.Len(t, got.Elems[1].Elems, 2)
	require.Equal(t, protocol.BulkOf("f1"), got.Elems[1].Elems[0])
	require.Equal(t, protocol.BulkOf("f2"), got.Elems[1].Elems[1])
	full := dispatch(r, "HSCAN", "h", "0")
	require.Len(t, full.Elems[1].Elems, 4)
}
