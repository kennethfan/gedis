// Package metrics 把 network.Stats 转成 Prometheus 文本 exposition 格式。
package metrics

import (
	"fmt"
	"net/http"
	"runtime"

	"github.com/kennethfan/gedis/internal/network"
)

// Handler 返回 /metrics 的 HTTP handler；stats 为 nil 时全零。
func Handler(stats *network.Stats) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		snap := stats.Snapshot()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, `# HELP gedis_uptime_seconds Server uptime in seconds.
# TYPE gedis_uptime_seconds gauge
gedis_uptime_seconds %d
# HELP gedis_connected_clients Currently connected clients.
# TYPE gedis_connected_clients gauge
gedis_connected_clients %d
# HELP gedis_blocked_clients Currently blocked clients.
# TYPE gedis_blocked_clients gauge
gedis_blocked_clients %d
# HELP gedis_connections_received_total Total accepted connections.
# TYPE gedis_connections_received_total counter
gedis_connections_received_total %d
# HELP gedis_commands_processed_total Total processed commands.
# TYPE gedis_commands_processed_total counter
gedis_commands_processed_total %d
# HELP gedis_used_memory_bytes Go heap bytes in use.
# TYPE gedis_used_memory_bytes gauge
gedis_used_memory_bytes %d
# HELP gedis_slowlog_length Current slow log length.
# TYPE gedis_slowlog_length gauge
gedis_slowlog_length %d
`, snap.UptimeSeconds, snap.ConnectedClients, snap.BlockedClients,
			snap.ConnsReceived, snap.CommandsProcessed, mem.HeapAlloc, stats.SlowLen())
	})
}
