package commands

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
)

const gedisVersion = "0.1.0"

// RegisterMonitor 注册 INFO 与 SLOWLOG；stats 为 nil 时计数类字段为零值。
// hub 为 nil 时 replication 小节取 Stats 快照（单测场景）。
func RegisterMonitor(r *network.Router, kv KV, stats *network.Stats, hub *replication.Hub) {
	h := &monitorHandler{kv: kv, stats: stats, hub: hub}
	r.Register("INFO", h.info)
	r.Register("SLOWLOG", h.slowlog)
	r.Register("CONFIG", h.config)
}

// MemoryInfo 是 INFO/CONFIG 需要的内存核算边界，*storage.Pebble 已满足。
type MemoryInfo interface {
	UsedBytes() int64
	MaxBytes() int64
	SetMaxBytes(n int64)
	Policy() string
	SetPolicy(policy string) error
	EvictedCount() int64
}

type monitorHandler struct {
	kv    KV
	stats *network.Stats
	hub   *replication.Hub
}

func (h *monitorHandler) info(ctx context.Context, args []protocol.Value) protocol.Value {
	section := "all"
	if len(args) > 1 {
		return errValueStr("ERR wrong number of arguments for 'info' command")
	}
	if len(args) == 1 {
		s, ok := argString(args[0])
		if !ok {
			return errValueStr("ERR invalid section")
		}
		section = strings.ToLower(s)
	}
	var b strings.Builder
	write := func(name string, body string) {
		if section != "all" && section != name {
			return
		}
		if b.Len() > 0 {
			b.WriteString("\r\n")
		}
		b.WriteString("# " + strings.Title(name) + "\r\n")
		b.WriteString(body)
	}
	snap := h.stats.Snapshot()
	write("server", h.serverSection(snap))
	write("clients", h.clientsSection(snap))
	write("memory", h.memorySection())
	write("stats", h.statsSection(snap))
	write("replication", h.replicationSection(snap))
	if section == "all" || section == "keyspace" {
		ks, err := h.keyspaceSection(ctx)
		if err != nil {
			return errValue(err)
		}
		if b.Len() > 0 {
			b.WriteString("\r\n")
		}
		b.WriteString(ks)
	}
	return protocol.BulkOf(b.String())
}

func (h *monitorHandler) serverSection(snap network.StatsView) string {
	var b strings.Builder
	fmt.Fprintf(&b, "redis_version:7.4.0\r\n")
	fmt.Fprintf(&b, "gedis_version:%s\r\n", gedisVersion)
	fmt.Fprintf(&b, "process_id:%d\r\n", os.Getpid())
	fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", snap.UptimeSeconds)
	fmt.Fprintf(&b, "uptime_in_days:%d\r\n", snap.UptimeSeconds/86400)
	fmt.Fprintf(&b, "hz:10\r\n")
	return b.String()
}

func (h *monitorHandler) clientsSection(snap network.StatsView) string {
	var b strings.Builder
	fmt.Fprintf(&b, "connected_clients:%d\r\n", snap.ConnectedClients)
	fmt.Fprintf(&b, "blocked_clients:%d\r\n", snap.BlockedClients)
	return b.String()
}

func (h *monitorHandler) memorySection() string {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	used := int64(mem.HeapAlloc)
	var maxBytes, evicted int64
	policy := "unknown"
	if mi, ok := h.kv.(MemoryInfo); ok && mi != nil {
		used = mi.UsedBytes()
		maxBytes = mi.MaxBytes()
		policy = mi.Policy()
		evicted = mi.EvictedCount()
	}
	var ratio float64
	if used > 0 {
		ratio = float64(mem.Sys) / float64(used)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "used_memory:%d\r\n", used)
	fmt.Fprintf(&b, "used_memory_rss:%d\r\n", mem.Sys)
	fmt.Fprintf(&b, "mem_fragmentation_ratio:%.2f\r\n", ratio)
	fmt.Fprintf(&b, "mem_allocator:go\r\n")
	fmt.Fprintf(&b, "maxmemory:%d\r\n", maxBytes)
	fmt.Fprintf(&b, "maxmemory_human:%s\r\n", humanBytes(maxBytes))
	fmt.Fprintf(&b, "maxmemory_policy:%s\r\n", policy)
	fmt.Fprintf(&b, "evicted_keys:%d\r\n", evicted)
	return b.String()
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fG", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fM", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2fK", float64(n)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func (h *monitorHandler) config(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'config' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "GET":
		if len(args) != 2 {
			return errValueStr("ERR wrong number of arguments for 'config|get' command")
		}
		pattern, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR invalid pattern")
		}
		mi, ok := h.kv.(MemoryInfo)
		if !ok || mi == nil {
			return errValueStr("ERR memory info unavailable")
		}
		params := [][2]string{
			{"maxmemory", strconv.FormatInt(mi.MaxBytes(), 10)},
			{"maxmemory-policy", mi.Policy()},
		}
		out := make([]protocol.Value, 0, 4)
		for _, p := range params {
			matched, merr := path.Match(strings.ToLower(pattern), p[0])
			if merr != nil || !matched {
				continue
			}
			out = append(out, protocol.BulkOf(p[0]), protocol.BulkOf(p[1]))
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	case "SET":
		if len(args) != 3 {
			return errValueStr("ERR wrong number of arguments for 'config|set' command")
		}
		name, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		val, ok := argString(args[2])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		mi, ok := h.kv.(MemoryInfo)
		if !ok || mi == nil {
			return errValueStr("ERR memory info unavailable")
		}
		switch strings.ToLower(name) {
		case "maxmemory":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return errValueStr("ERR value is not an integer or out of range")
			}
			mi.SetMaxBytes(n)
			return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
		case "maxmemory-policy":
			if err := mi.SetPolicy(val); err != nil {
				return errValue(err)
			}
			return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
		default:
			return errValueStr("ERR Unknown parameter '" + name + "'")
		}
	default:
		return errValueStr("ERR unknown subcommand for 'config' command")
	}
}

func (h *monitorHandler) statsSection(snap network.StatsView) string {
	var b strings.Builder
	fmt.Fprintf(&b, "total_connections_received:%d\r\n", snap.ConnsReceived)
	fmt.Fprintf(&b, "total_commands_processed:%d\r\n", snap.CommandsProcessed)
	fmt.Fprintf(&b, "expired_keys:%d\r\n", snap.ExpiredKeys)
	return b.String()
}

func (h *monitorHandler) replicationSection(snap network.StatsView) string {
	var b strings.Builder
	if snap.Role == "slave" {
		fmt.Fprintf(&b, "role:slave\r\n")
		fmt.Fprintf(&b, "master_host:%s\r\n", snap.MasterHost)
		fmt.Fprintf(&b, "master_port:%d\r\n", snap.MasterPort)
		fmt.Fprintf(&b, "master_link_status:up\r\n")
		fmt.Fprintf(&b, "slave_repl_offset:%d\r\n", snap.SlaveOffset)
		return b.String()
	}
	var slaves, offset int64
	if h.hub != nil {
		slaves = h.hub.SubCount()
		offset = h.hub.Backlog().Latest()
	}
	fmt.Fprintf(&b, "role:master\r\n")
	fmt.Fprintf(&b, "connected_slaves:%d\r\n", slaves)
	fmt.Fprintf(&b, "master_replid:%s\r\n", snap.ReplID)
	fmt.Fprintf(&b, "master_repl_offset:%d\r\n", offset)
	fmt.Fprintf(&b, "master_replid2:0000000000000000000000000000000000000000\r\n")
	fmt.Fprintf(&b, "second_repl_offset:-1\r\n")
	return b.String()
}

// keyspaceSection 扫描全部类型前缀统计 keys/expires/avg_ttl；
// 已过期条目按读语义跳过（不删除，INFO 无副作用）。
func (h *monitorHandler) keyspaceSection(ctx context.Context) (string, error) {
	var keys, expires int64
	var ttlSum int64
	now := time.Now().UnixNano()
	_, raws, err := allUserKeys(ctx, h.kv)
	if err != nil {
		return "", err
	}
	for _, k := range raws {
		v, err := h.kv.Get(ctx, k)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return "", err
		}
		e, err := datastruct.Decode(v)
		if err != nil {
			return "", err
		}
		if e.Expiry != 0 && now >= e.Expiry {
			continue
		}
		keys++
		if e.Expiry != 0 {
			expires++
			ttlSum += (e.Expiry - now) / int64(time.Millisecond)
		}
	}
	var avg int64
	if expires > 0 {
		avg = ttlSum / expires
	}
	return fmt.Sprintf("# Keyspace\r\ndb0:keys=%d,expires=%d,avg_ttl=%d\r\n", keys, expires, avg), nil
}

func (h *monitorHandler) slowlog(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'slowlog' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "LEN":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'slowlog' command")
		}
		return protocol.Value{Kind: protocol.KindInteger, I: int64(h.stats.SlowLen())}
	case "RESET":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'slowlog' command")
		}
		h.stats.SlowReset()
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "GET":
		count := int64(128)
		if len(args) > 2 {
			return errValueStr("ERR wrong number of arguments for 'slowlog' command")
		}
		if len(args) == 2 {
			n, ok := argString(args[1])
			if !ok {
				return errValueStr("ERR value is not an integer or out of range")
			}
			var err error
			count, err = strconv.ParseInt(n, 10, 64)
			if err != nil || count < 0 {
				return errValueStr("ERR value is not an integer or out of range")
			}
		}
		entries := h.stats.SlowEntries(int(count))
		out := make([]protocol.Value, 0, len(entries))
		for _, e := range entries {
			cmd := make([]protocol.Value, 0, len(e.Args)+1)
			cmd = append(cmd, protocol.BulkOf(e.Command))
			for _, a := range e.Args {
				cmd = append(cmd, protocol.BulkOf(a))
			}
			out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
				{Kind: protocol.KindInteger, I: e.ID},
				{Kind: protocol.KindInteger, I: e.Timestamp},
				{Kind: protocol.KindInteger, I: e.DurationMicros},
				{Kind: protocol.KindArray, Elems: cmd},
			}})
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	default:
		return errValueStr("ERR unknown subcommand for 'slowlog' command")
	}
}
