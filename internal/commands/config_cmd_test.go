package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 默认无限制
// When: CONFIG SET maxmemory / GET
// Then: 运行时生效并可读回；非法策略报错
func Test_Config_when_SetGetMaxmemory(t *testing.T) {
	r, store, _ := openMonitorSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "CONFIG", "SET", "maxmemory", "1024"))
	require.Equal(t, int64(1024), store.MaxBytes())
	got := dispatch(r, "CONFIG", "GET", "maxmemory")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Equal(t, protocol.BulkOf("maxmemory"), got.Elems[0])
	require.Equal(t, protocol.BulkOf("1024"), got.Elems[1])

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "CONFIG", "SET", "maxmemory-policy", "volatile-lru"))
	require.Equal(t, "volatile-lru", store.Policy())
	got = dispatch(r, "CONFIG", "GET", "maxmemory-policy")
	require.Equal(t, protocol.BulkOf("volatile-lru"), got.Elems[1])

	got = dispatch(r, "CONFIG", "SET", "maxmemory-policy", "bogus")
	require.Equal(t, protocol.KindError, got.Kind)
	got = dispatch(r, "CONFIG", "SET", "nosuch", "1")
	require.Equal(t, protocol.KindError, got.Kind)
}

// Given: CONFIG 设小内存上限
// When: 写入超限
// Then: 发生驱逐且 used 有界；volatile 下全持久 key 写报 OOM
func Test_Memory_when_ConfigDrivenEviction(t *testing.T) {
	r, store, _ := openMonitorSetup(t)
	dispatch(r, "CONFIG", "SET", "maxmemory", "200")
	big := string(make([]byte, 50))
	for i := 0; i < 10; i++ {
		dispatch(r, "SET", "k", big)
		dispatch(r, "SET", "k"+string(rune('a'+i)), big)
	}
	require.Greater(t, store.EvictedCount(), int64(0))
	require.LessOrEqual(t, store.UsedBytes(), int64(200+128))

	dispatch(r, "CONFIG", "SET", "maxmemory-policy", "volatile-lru")
	dispatch(r, "CONFIG", "SET", "maxmemory", "50")
	got := dispatch(r, "SET", "oom", big)
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "OOM")
}

// Given: 写入一些 key
// When: INFO memory
// Then: maxmemory/used_memory/fragmentation/evicted_keys 字段齐全
func Test_Config_when_InfoMemory(t *testing.T) {
	r, _, _ := openMonitorSetup(t)
	dispatch(r, "SET", "k", "v")
	got := dispatch(r, "INFO", "memory")
	out := string(got.Bulk)
	for _, marker := range []string{
		"used_memory:", "maxmemory:", "maxmemory_policy:",
		"mem_fragmentation_ratio:", "evicted_keys:",
	} {
		require.Contains(t, out, marker)
	}
}
