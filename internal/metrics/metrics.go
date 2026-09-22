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
		fmt.Fprintf(w, `# HELP redrock_uptime_seconds Server uptime in seconds.
# TYPE redrock_uptime_seconds gauge
redrock_uptime_seconds %d
# HELP redrock_connected_clients Currently connected clients.
# TYPE redrock_connected_clients gauge
redrock_connected_clients %d
# HELP redrock_blocked_clients Currently blocked clients.
# TYPE redrock_blocked_clients gauge
redrock_blocked_clients %d
# HELP redrock_connections_received_total Total accepted connections.
# TYPE redrock_connections_received_total counter
redrock_connections_received_total %d
# HELP redrock_commands_processed_total Total processed commands.
# TYPE redrock_commands_processed_total counter
redrock_commands_processed_total %d
# HELP redrock_used_memory_bytes Go heap bytes in use.
# TYPE redrock_used_memory_bytes gauge
redrock_used_memory_bytes %d
# HELP redrock_slowlog_length Current slow log length.
# TYPE redrock_slowlog_length gauge
redrock_slowlog_length %d
`, snap.UptimeSeconds, snap.ConnectedClients, snap.BlockedClients,
			snap.ConnsReceived, snap.CommandsProcessed, mem.HeapAlloc, stats.SlowLen())
	})
}
