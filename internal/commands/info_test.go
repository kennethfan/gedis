package commands

import (
	"strings"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openMonitorSetup(t testing.TB) (*network.Router, *storage.Pebble, *network.Stats) {
	t.Helper()
	r, store := openTestSetup(t)
	s := network.NewStats()
	r.AttachStats(s)
	RegisterSet(r, store)
	RegisterMonitor(r, store, s, nil)
	return r, store, s
}

// Given: SET + SADD 已写入
// When: INFO 无参数
// Then: 六个 section 都在，keyspace 计数正确
func Test_Monitor_when_InfoAll(t *testing.T) {
	r, _, _ := openMonitorSetup(t)
	dispatch(r, "SET", "str", "v")
	dispatch(r, "SADD", "set", "a")
	got := dispatch(r, "INFO")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	out := string(got.Bulk)
	for _, marker := range []string{
		"# Server", "redis_version:", "# Clients", "connected_clients:",
		"# Memory", "used_memory:", "# Stats", "total_commands_processed:", "expired_keys:",
		"# Replication", "role:master", "# Keyspace", "db0:keys=2",
	} {
		require.Contains(t, out, marker)
	}
}

// Given: 空库
// When: INFO stats（小写 section 名）/ INFO nosuch
// Then: 只返回对应 section；未知 section 返回空
func Test_Monitor_when_InfoSection(t *testing.T) {
	r, _, _ := openMonitorSetup(t)
	got := dispatch(r, "INFO", "stats")
	out := string(got.Bulk)
	require.Contains(t, out, "# Stats")
	require.NotContains(t, out, "# Server")
	got = dispatch(r, "INFO", "nosuch")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.True(t, len(got.Bulk) == 0 || strings.HasPrefix(string(got.Bulk), "#"))
}

// Given: 慢日志阈值为 0
// When: PING 后 SLOWLOG LEN / GET / RESET
// Then: 记录形状为 [id timestamp micros [cmd args]]，RESET 清空
func Test_Monitor_when_Slowlog(t *testing.T) {
	r, _, s := openMonitorSetup(t)
	s.SlowThresholdMicros = 0
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SLOWLOG", "LEN"))
	dispatch(r, "SET", "k", "v")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SLOWLOG", "LEN"))
	got := dispatch(r, "SLOWLOG", "GET")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	entry := got.Elems[0]
	require.Equal(t, protocol.KindArray, entry.Kind)
	require.Len(t, entry.Elems, 4)
	require.Equal(t, int64(1), entry.Elems[0].I)
	require.Equal(t, "set", string(entry.Elems[3].Elems[0].Bulk))
	require.Equal(t, "k", string(entry.Elems[3].Elems[1].Bulk))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SLOWLOG", "RESET"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SLOWLOG", "LEN"))
}
