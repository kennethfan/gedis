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
	r.Register("BGSAVE", h.unsupportedPersistence)
	r.Register("BGREWRITEAOF", h.unsupportedPersistence)
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
}

type serverHandler struct {
	kv    KV
	stats *network.Stats
	hub   *replication.Hub
	conns *ConnRegistry
	deps  ServerDeps
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
	return protocol.Value{Kind: protocol.KindInteger, I: h.deps.StartUnix}
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

// client 实现 CLIENT 子命令：LIST/GETNAME/SETNAME 走 ConnRegistry，
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
	case "KILL":
		return errValueStr("ERR CLIENT KILL not supported (no connection closer wired)")
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s' for 'client' command", sub))
	}
}

// memory 实现 MEMORY USAGE（估算：entry 字节 + 64 常数开销，
// Pebble 无精确单 key 内存）；DOCTOR/STATS/MALLOC-STATS 明确不支持。
func (h *serverHandler) memory(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'memory' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	if !strings.EqualFold(sub, "USAGE") {
		return errValueStr(fmt.Sprintf("ERR MEMORY %s not supported on this engine", strings.ToUpper(sub)))
	}
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

// latency 无延迟跟踪：查询回空数组，RESET 回 OK。
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
	case "RESET":
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s' for 'latency' command", sub))
	}
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

// unsupportedPersistence 诚实失败：本引擎无 RDB/AOF，静默 +OK 是谎言。
func (h *serverHandler) unsupportedPersistence(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for persistence command")
	}
	return errValueStr("ERR not supported on this engine: no RDB/AOF persistence")
}
