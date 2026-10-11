package commands

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
)

// ServerDeps 是服务端命令的装配参数：StartUnix 供 LASTSAVE/持久化
// 信息用；Shutdown 为 SHUTDOWN 的优雅停机钩（main 接 expirer+listener），
// 单测可注入 spy，nil 时仅回 OK 不动作。
type ServerDeps struct {
	StartUnix int64
	Shutdown  func()
}

// RegisterServer 注册服务端与运维命令，返回 handler（调用方一般忽略）。
func RegisterServer(r *network.Router, kv KV, stats *network.Stats, hub *replication.Hub, conns *ConnRegistry, deps ServerDeps) *serverHandler {
	for _, m := range serverMeta {
		acl.RegisterMeta(m)
	}
	h := &serverHandler{kv: kv, stats: stats, hub: hub, conns: conns, deps: deps}
	h.tracks = NewTrackTable()
	conns.SetTrackTable(h.tracks)
	InstallTrackingHook(r, h.conns, h.tracks)
	InstallInvalidation(h.conns, h.tracks)
	r.Register("DBSIZE", h.dbsize)
	r.Register("FLUSHDB", h.flushdb)
	r.Register("FLUSHALL", h.flushall)
	r.Register("LASTSAVE", h.lastsave)
	r.Register("ROLE", h.role)
	r.Register("SHUTDOWN", h.shutdown)
	r.Register("TIME", h.time_)
	r.Register("CLIENT", h.client)
	r.Register("MEMORY", h.memory)
	r.Register("LATENCY", h.latency)
	r.Register("DEBUG", h.debug)
	r.Register("BGSAVE", saveTracker.bgsaveCmd)
	r.Register("BGREWRITEAOF", h.bgrewriteaof)
	r.Register("SAVE", saveTracker.saveCmd)
	r.Register("MONITOR", h.monitor)
	InstallMonitorHook(r)
	saveTracker.resetStartUnix(deps.StartUnix)
	return h
}

var serverMeta = []acl.Meta{
	{Name: "DBSIZE", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{First: -1}},
	{Name: "FLUSHDB", Category: "keyspace", Keys: acl.KeySpec{First: -1}},
	{Name: "FLUSHALL", Category: "keyspace", Keys: acl.KeySpec{First: -1}},
	{Name: "LASTSAVE", Category: "server", ReadOnly: true, Keys: acl.KeySpec{First: -1}},
	{Name: "ROLE", Category: "server", ReadOnly: true, Keys: acl.KeySpec{First: -1}},
	{Name: "SHUTDOWN", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "TIME", Category: "server", ReadOnly: true, Keys: acl.KeySpec{First: -1}},
	{Name: "CLIENT", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "MEMORY", Category: "server", ReadOnly: true, Keys: acl.KeySpec{First: -1}},
	{Name: "LATENCY", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "DEBUG", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "BGSAVE", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "BGREWRITEAOF", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "SAVE", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "MONITOR", Category: "admin", Keys: acl.KeySpec{First: -1}},
}

type serverHandler struct {
	kv     KV
	stats  *network.Stats
	hub    *replication.Hub
	conns  *ConnRegistry
	deps   ServerDeps
	tracks *TrackTable
}

func (h *serverHandler) monitor(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'monitor' command")
	}
	return startMonitor(ctx)
}

// dbsize 统计未过期 key 数（读语义：跳过已过期条目，无副作用）。
func (h *serverHandler) dbsize(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'dbsize' command")
	}
	_, raws, err := allUserKeys(ctx, h.kv)
	if err != nil {
		return errValue(err)
	}
	now := time.Now().UnixNano()
	var n int64
	for _, k := range raws {
		v, err := h.kv.Get(ctx, k)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return errValue(err)
		}
		e, err := datastruct.Decode(v)
		if err != nil {
			return errValue(err)
		}
		if e.Expiry != 0 && now >= e.Expiry {
			continue
		}
		n++
	}
	return protocol.Value{Kind: protocol.KindInteger, I: n}
}

// flush 清空全库：逐 key 删除并逐条向 Hub 发布 del 标记，复本经现有
// OP 路径收敛（无需新帧类型）；backlog 水位随之推进。
func (h *serverHandler) flush(ctx context.Context, args []protocol.Value, name string) protocol.Value {
	if len(args) > 1 {
		return errValueStr(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
	}
	if len(args) == 1 {
		mode, ok := argString(args[0])
		if !ok || !strings.EqualFold(mode, "ASYNC") {
			return errValueStr("ERR syntax error")
		}
	}
	_, raws, err := allUserKeys(ctx, h.kv)
	if err != nil {
		return errValue(err)
	}
	for _, k := range raws {
		if err := h.kv.Delete(ctx, k); err != nil {
			return errValue(err)
		}
		if h.hub != nil {
			h.hub.Publish(replication.Op{Del: true, Key: k})
		}
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *serverHandler) flushdb(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.flush(ctx, args, "FLUSHDB")
}

func (h *serverHandler) flushall(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.flush(ctx, args, "FLUSHALL")
}

func (h *serverHandler) lastsave(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'lastsave' command")
	}
	return protocol.Value{Kind: protocol.KindInteger, I: saveTracker.LastSave()}
}

// role 按 Stats 快照报主从角色；主返回 backlog 最新 offset，
// 从返回主地址与已应用 offset。
func (h *serverHandler) role(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'role' command")
	}
	var snap network.StatsView
	if h.stats != nil {
		snap = h.stats.Snapshot()
	}
	if snap.Role == "slave" {
		return protocol.ArrayOf(
			protocol.BulkOf("slave"),
			protocol.BulkOf(snap.MasterHost),
			protocol.Value{Kind: protocol.KindInteger, I: int64(snap.MasterPort)},
			protocol.BulkOf("connected"),
			protocol.Value{Kind: protocol.KindInteger, I: snap.SlaveOffset},
		)
	}
	var offset int64
	if h.hub != nil {
		offset = h.hub.Backlog().Latest()
	}
	return protocol.ArrayOf(
		protocol.BulkOf("master"),
		protocol.Value{Kind: protocol.KindInteger, I: offset},
		protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}},
	)
}

// shutdown 接受 [NOSAVE|SAVE]（SAVE 为 no-op：Pebble 常驻持久），
// 回 OK 后异步停机并标记本连接关闭。
func (h *serverHandler) shutdown(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) > 1 {
		return errValueStr("ERR wrong number of arguments for 'shutdown' command")
	}
	if len(args) == 1 {
		mode, ok := argString(args[0])
		if !ok || (!strings.EqualFold(mode, "NOSAVE") && !strings.EqualFold(mode, "SAVE")) {
			return errValueStr("ERR syntax error")
		}
	}
	if conn, ok := network.ConnFromContext(ctx); ok && conn != nil {
		network.RequestClose(conn)
	}
	if h.deps.Shutdown != nil {
		go func() {
			time.Sleep(100 * time.Millisecond)
			h.deps.Shutdown()
		}()
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *serverHandler) time_(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'time' command")
	}
	now := time.Now()
	return protocol.ArrayOf(
		protocol.BulkOf(strconv.FormatInt(now.Unix(), 10)),
		protocol.BulkOf(strconv.FormatInt(int64(now.Nanosecond()/1000), 10)),
	)
}

// client 实现 CLIENT 子命令：LIST/GETNAME/SETNAME/SETINFO 走 ConnRegistry，
// ID 取本连接序号；KILL 尚未接线，诚实报错。
func (h *serverHandler) client(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'client' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "LIST":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'client|list' command")
		}
		if h.conns == nil {
			return protocol.BulkOf("")
		}
		// 先给本连接建号：直连就发 LIST 的客户端此前未进表，不补会看不到自己。
		if conn, ok := network.ConnFromContext(ctx); ok && conn != nil {
			_ = h.conns.IDOf(conn)
		}
		var b strings.Builder
		for _, c := range h.conns.Snapshot() {
			fmt.Fprintf(&b, "id=%d addr=%s name=%s proto=%d\n", c.ID, c.Addr, c.Name, c.Proto)
		}
		return protocol.BulkOf(b.String())
	case "SETNAME":
		if len(args) != 2 {
			return errValueStr("ERR wrong number of arguments for 'client|setname' command")
		}
		conn, ok := network.ConnFromContext(ctx)
		if !ok || conn == nil || h.conns == nil {
			return errValueStr("ERR no connection to set name on")
		}
		name, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		h.conns.SetName(conn, name)
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "GETNAME":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'client|getname' command")
		}
		conn, ok := network.ConnFromContext(ctx)
		if !ok || conn == nil || h.conns == nil {
			return errValueStr("ERR no connection to get name of")
		}
		if name := h.conns.NameOf(conn); name != "" {
			return protocol.BulkOf(name)
		}
		return protocol.Value{Kind: protocol.KindBulkString}
	case "ID":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'client|id' command")
		}
		conn, ok := network.ConnFromContext(ctx)
		if !ok || conn == nil || h.conns == nil {
			return errValueStr("ERR no connection for client id")
		}
		return protocol.Value{Kind: protocol.KindInteger, I: h.conns.IDOf(conn)}
	case "SETINFO":
		if len(args) != 3 {
			return errValueStr("ERR wrong number of arguments for 'client|setinfo' command")
		}
		sec, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		up := strings.ToUpper(sec)
		if up != "LIB-NAME" && up != "LIB-VER" {
			return errValueStr(fmt.Sprintf("ERR Unrecognized option '%s'", sec))
		}
		val, ok := argString(args[2])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		conn, ok := network.ConnFromContext(ctx)
		if !ok || conn == nil || h.conns == nil {
			return errValueStr("ERR no connection to set info on")
		}
		h.conns.SetLibInfo(conn, up, val)
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "KILL":
		return errValueStr("ERR CLIENT KILL not supported (no connection closer wired)")
	case "MONITOR":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'client|monitor' command")
		}
		return startMonitor(ctx)
	case "TRACKING":
		return h.tracking(ctx, args[1:])
	case "CACHING":
		return h.caching(ctx, args[1:])
	case "HELP":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'client|help' command")
		}
		return clientHelp()
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s' for 'client' command", sub))
	}
}

// memory 实现 MEMORY USAGE/DOCTOR/PURGE/HELP；STATS/MALLOC-STATS
// 无 allocator 可报告，维持诚实错（显性偏差，文档声明）。
func (h *serverHandler) memory(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'memory' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "USAGE":
		return h.memoryUsage(ctx, args)
	case "DOCTOR":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'memory|doctor' command")
		}
		return protocol.BulkOf(memoryDoctorText)
	case "PURGE":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'memory|purge' command")
		}
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "HELP":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'memory|help' command")
		}
		return memoryHelp()
	default:
		return errValueStr(fmt.Sprintf("ERR MEMORY %s not supported on this engine", strings.ToUpper(sub)))
	}
}
// memoryUsage MEMORY USAGE（估算：entry 字节 + 64 常数开销，
// Pebble 无精确单 key 内存）。
func (h *serverHandler) memoryUsage(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 && len(args) != 4 {
		return errValueStr("ERR wrong number of arguments for 'memory|usage' command")
	}
	if len(args) == 4 {
		opt, ok := argString(args[2])
		if !ok || !strings.EqualFold(opt, "SAMPLES") {
			return errValueStr("ERR syntax error")
		}
		if _, err := strconv.ParseInt(mustArgString(args[3]), 10, 64); err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
	}
	key, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	raw, _, err := lookupRaw(ctx, h.kv, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	v, err := h.kv.Get(ctx, raw)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(v) + 64)}
}

func mustArgString(v protocol.Value) string {
	s, _ := argString(v)
	return s
}

// latency 无延迟跟踪：查询回空数组，RESET 回 OK；DOCTOR 回 Dave 文，
// GRAPH 恒无样本报错，HISTOGRAM 恒空数组，HELP 照抄真机。
func (h *serverHandler) latency(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'latency' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "LATEST", "HISTORY":
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	case "HISTOGRAM":
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	case "RESET":
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "DOCTOR":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'latency|doctor' command")
		}
		return protocol.BulkOf(latencyDoctorText)
	case "GRAPH":
		if len(args) != 2 {
			return errValueStr("ERR wrong number of arguments for 'latency|graph' command")
		}
		ev, _ := argString(args[1])
		return errValueStr(fmt.Sprintf("ERR No samples available for event '%s'", ev))
	case "HELP":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'latency|help' command")
		}
		return latencyHelp()
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s' for 'latency' command", sub))
	}
}

// memoryDoctorText/latencyDoctorText 真机原文（7.2.6 探针逐字）：MEMORY
// DOCTOR 恒回 Sam 文（无 allocator 可报告，显性偏差）；LATENCY DOCTOR
// 恒回 Dave 文（无延迟跟踪，与 fresh 真机关闭态一致）。
const memoryDoctorText = "Hi Sam, this instance is empty or is using very little memory, my issues detector can't be used in these conditions. Please, leave for your mission on Earth and fill it with some data. The new Sam and I will be back to our programming as soon as I finished rebooting."

const latencyDoctorText = "I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this Redis instance. You may use \"CONFIG SET latency-monitor-threshold <milliseconds>.\" in order to enable it. If we weren't in a deep space mission I'd suggest to take a look at https://redis.io/topics/latency-monitor."

// clientHelpLines CLIENT HELP 全文（7.2.6 探针逐字）。
var clientHelpLines = []string{
	"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CACHING (YES|NO)",
	"    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
	"GETREDIR",
	"    Return the client ID we are redirecting to when tracking is enabled.",
	"GETNAME",
	"    Return the name of the current connection.",
	"ID",
	"    Return the ID of the current connection.",
	"INFO",
	"    Return information about the current client connection.",
	"KILL <ip:port>",
	"    Kill connection made from <ip:port>.",
	"KILL <option> <value> [<option> <value> [...]]",
	"    Kill connections. Options are:",
	"    * ADDR (<ip:port>|<unixsocket>:0)",
	"      Kill connections made from the specified address",
	"    * LADDR (<ip:port>|<unixsocket>:0)",
	"      Kill connections made to specified local address",
	"    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
	"      Kill connections by type.",
	"    * USER <username>",
	"      Kill connections authenticated by <username>.",
	"    * SKIPME (YES|NO)",
	"      Skip killing current connection (default: yes).",
	"LIST [options ...]",
	"    Return information about client connections. Options:",
	"    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
	"      Return clients of specified type.",
	"UNPAUSE",
	"    Stop the current client pause, resuming traffic.",
	"PAUSE <timeout> [WRITE|ALL]",
	"    Suspend all, or just write, clients for <timeout> milliseconds.",
	"REPLY (ON|OFF|SKIP)",
	"    Control the replies sent to the current connection.",
	"SETNAME <name>",
	"    Assign the name <name> to the current connection.",
	"SETINFO <option> <value>",
	"    Set client meta attr. Options are:",
	"    * LIB-NAME: the client lib name.",
	"    * LIB-VER: the client lib version.",
	"UNBLOCK <clientid> [TIMEOUT|ERROR]",
	"    Unblock the specified blocked client.",
	"TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
	"         [OPTIN] [OPTOUT] [NOLOOP]",
	"    Control server assisted client side caching.",
	"TRACKINGINFO",
	"    Report tracking status for the current connection.",
	"NO-EVICT (ON|OFF)",
	"    Protect current client connection from eviction.",
	"NO-TOUCH (ON|OFF)",
	"    Will not touch LRU/LFU stats when this mode is on.",
	"HELP",
	"    Print this help.",
}

func clientHelp() protocol.Value {
	out := make([]protocol.Value, 0, len(clientHelpLines))
	for _, s := range clientHelpLines {
		out = append(out, protocol.Value{Kind: protocol.KindSimpleString, S: s})
	}
	return protocol.ArrayOf(out...)
}

// memoryHelpLines MEMORY HELP 全文（7.2.6 探针逐字）。
var memoryHelpLines = []string{
	"MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"DOCTOR",
	"    Return memory problems reports.",
	"MALLOC-STATS",
	"    Return internal statistics report from the memory allocator.",
	"PURGE",
	"    Attempt to purge dirty pages for reclamation by the allocator.",
	"STATS",
	"    Return information about the memory usage of the server.",
	"USAGE <key> [SAMPLES <count>]",
	"    Return memory in bytes used by <key> and its value. Nested values are",
	"    sampled up to <count> times (default: 5, 0 means sample all).",
	"HELP",
	"    Print this help.",
}

func memoryHelp() protocol.Value {
	out := make([]protocol.Value, 0, len(memoryHelpLines))
	for _, s := range memoryHelpLines {
		out = append(out, protocol.Value{Kind: protocol.KindSimpleString, S: s})
	}
	return protocol.ArrayOf(out...)
}

// latencyHelpLines LATENCY HELP 全文（7.2.6 探针逐字）。
var latencyHelpLines = []string{
	"LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"DOCTOR",
	"    Return a human readable latency analysis report.",
	"GRAPH <event>",
	"    Return an ASCII latency graph for the <event> class.",
	"HISTORY <event>",
	"    Return time-latency samples for the <event> class.",
	"LATEST",
	"    Return the latest latency samples for all events.",
	"RESET [<event> ...]",
	"    Reset latency data of one or more <event> classes.",
	"    (default: reset all data for all event classes)",
	"HISTOGRAM [COMMAND ...]",
	"    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
	"    If no commands are specified then all histograms are replied.",
	"HELP",
	"    Print this help.",
}

func latencyHelp() protocol.Value {
	out := make([]protocol.Value, 0, len(latencyHelpLines))
	for _, s := range latencyHelpLines {
		out = append(out, protocol.Value{Kind: protocol.KindSimpleString, S: s})
	}
	return protocol.ArrayOf(out...)
}

// debug 实现 SLEEP（阻塞本连接 handler）、OBJECT（编码快照）、
// RELOAD（无 RDB，no-op 回 OK）。
func (h *serverHandler) debug(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'debug' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "SLEEP":
		if len(args) != 2 {
			return errValueStr("ERR wrong number of arguments for 'debug|sleep' command")
		}
		sec, err := strconv.ParseFloat(mustArgString(args[1]), 64)
		if err != nil || sec < 0 {
			return errValueStr("ERR value is not a valid float")
		}
		time.Sleep(time.Duration(sec * float64(time.Second)))
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "OBJECT":
		if len(args) != 2 {
			return errValueStr("ERR wrong number of arguments for 'debug|object' command")
		}
		key, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR invalid key")
		}
		raw, e, err := lookupRaw(ctx, h.kv, key)
		if err != nil {
			if isNotFound(err) {
				return protocol.Value{Kind: protocol.KindBulkString}
			}
			return errValue(err)
		}
		v, err := h.kv.Get(ctx, raw)
		if err != nil {
			if isNotFound(err) {
				return protocol.Value{Kind: protocol.KindBulkString}
			}
			return errValue(err)
		}
		enc, ok := objectEncodingOf(e)
		if !ok {
			enc = "unknown"
		}
		return protocol.BulkOf(fmt.Sprintf("encoding:%s serializedlength:%d", enc, len(v)))
	case "RELOAD":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'debug|reload' command")
		}
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s' for 'debug' command", sub))
	}
}

// bgrewriteaof 诚实失败：本引擎无 AOF，静默 +OK 是谎言（P2 偏差，文档注明）。
func (h *serverHandler) bgrewriteaof(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for persistence command")
	}
	return errValueStr("ERR not supported on this engine: no AOF persistence")
}
