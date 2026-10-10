package commands

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
)

// RegisterReplication 注册 PSYNC（主库侧）与 REPLICAOF；REPLICAOF 启停
// 复本客户端并切换只读模式与 Stats 角色。
// REPLCONF 是副本挂载与 ACK 上报入口；WAIT 读副本 ACK 水位；SYNC 明确
// 不支持（指路 PSYNC）；SLAVEOF 为 REPLICAOF 别名；FAILOVER 无副本时明确报错。
var replicationMeta = []acl.Meta{
	{Name: "PSYNC", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "REPLICAOF", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "SLAVEOF", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "REPLCONF", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "WAIT", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "SYNC", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "FAILOVER", Category: "admin", Keys: acl.KeySpec{First: -1}},
}

func RegisterReplication(r *network.Router, kv KV, stats *network.Stats, hub *replication.Hub) {
	for _, m := range replicationMeta {
		acl.RegisterMeta(m)
	}
	h := &replHandler{router: r, kv: kv, stats: stats, hub: hub}
	r.Register("PSYNC", h.psync)
	r.Register("REPLICAOF", h.replicaof)
	r.Register("SLAVEOF", h.replicaof)
	r.Register("REPLCONF", h.replconf)
	r.Register("WAIT", h.wait)
	r.Register("SYNC", func(context.Context, []protocol.Value) protocol.Value {
		return errValueStr("ERR SYNC is deprecated, use PSYNC")
	})
	r.Register("FAILOVER", h.failover)
}

type replHandler struct {
	router *network.Router
	kv     KV
	stats  *network.Stats
	hub    *replication.Hub
	client *replication.Client

	// replicas 是已完成 PSYNC 建链的副本连接 → 其 ACK 上报的最大 offset。
	// WAIT 读此表计数；sendLoop 退出时注销。只统计建链连接，普通客户端
	// 冒发的 ACK 不计入（防虚报）。
	mu       sync.Mutex
	replicas map[net.Conn]int64
}

func (h *replHandler) replicaof(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'replicaof' command")
	}
	first, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	second, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	if strings.EqualFold(first, "NO") && strings.EqualFold(second, "ONE") {
		if h.client != nil {
			h.client.Stop()
			h.client = nil
		}
		h.router.SetReadOnly(false)
		h.stats.ClearMaster()
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	}
	port, err := strconv.Atoi(second)
	if err != nil || port <= 0 || port > 65535 {
		return errValueStr("ERR value is not an integer or out of range")
	}
	if h.client != nil {
		h.client.Stop()
	}
	h.stats.SetMaster(first, port)
	h.router.SetReadOnly(true)
	h.client = replication.NewClient(h.kv, net.JoinHostPort(first, second))
	h.client.OnOffset = h.stats.SetSlaveOffset
	h.client.Start()
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

// psync 处理复本的同步请求：
//   - PSYNC ? -1（或 replid 失配 / offset 过期）→ 全量：[FULLRESYNC 标记, RDB bulk]，随后增量流
//   - PSYNC <replid> <offset>（命中 backlog）→ 部分：[CONTINUE 标记, 遗漏 OP 数组]，随后增量流
//
// 增量流由 sender goroutine 直写连接；连接断开即退订。
func (h *replHandler) psync(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'psync' command")
	}
	conn, ok := network.ConnFromContext(ctx)
	if !ok {
		return errValueStr("ERR PSYNC requires a connection")
	}
	replid, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid replication id")
	}
	offStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid offset")
	}
	offset, err := strconv.ParseInt(offStr, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}

	masterID := h.stats.ReplID
	if replid != "?" && replid == masterID {
		if id, ch, missed := h.hub.SubscribeSince(offset); id != 0 {
			untrack := h.trackReplica(conn)
			go h.sendLoop(conn, id, ch, untrack)
			return protocol.ArrayOf(
				protocol.BulkOf("CONTINUE "+masterID),
				encodeOps(missed),
			)
		}
	}
	id, ch := h.hub.Subscribe()
	entries, rerr := h.gatherRDB(ctx)
	if rerr != nil {
		h.hub.Unsubscribe(id)
		return errValue(rerr)
	}
	untrack := h.trackReplica(conn)
	go h.sendLoop(conn, id, ch, untrack)
	return protocol.ArrayOf(
		protocol.BulkOf(fmt.Sprintf("FULLRESYNC %s %d", masterID, h.hub.Backlog().Latest())),
		protocol.Value{Kind: protocol.KindBulkString, Bulk: replication.MarshalRDB(entries)},
	)
}

// gatherRDB 收集全库 raw KV（读多写少场景一次快照；与随后增量流重复投递幂等）。
func (h *replHandler) gatherRDB(ctx context.Context) ([]replication.RawEntry, error) {
	_, raws, err := allUserKeys(ctx, h.kv)
	if err != nil {
		return nil, err
	}
	out := make([]replication.RawEntry, 0, len(raws))
	for _, k := range raws {
		v, err := h.kv.Get(ctx, k)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, err
		}
		out = append(out, replication.RawEntry{Key: k, Value: v})
	}
	return out, nil
}

// sendLoop 把订阅到的操作编码成 OP 数组直写复本连接；写失败即退订退出。
// untrack 在退出时注销该副本连接的 ACK 记录（WAIT 不再计数它）。
func (h *replHandler) sendLoop(conn net.Conn, id int64, ch <-chan []replication.Op, untrack func()) {
	defer func() {
		h.hub.Unsubscribe(id)
		untrack()
	}()
	for batch := range ch {
		for _, op := range batch {
			if _, err := conn.Write(encodeOp(op).Append(nil)); err != nil {
				return
			}
		}
	}
}

// encodeOp 把操作编码成 ["OP","set",key,value,offset] 或 ["OP","del",key,offset]
// 数组（key/value 二进制安全；offset 供复本断点续传）。
func encodeOp(op replication.Op) protocol.Value {
	off := protocol.BulkOf(strconv.FormatInt(op.Offset, 10))
	if op.Del {
		return protocol.ArrayOf(
			protocol.BulkOf("OP"),
			protocol.BulkOf("del"),
			protocol.Value{Kind: protocol.KindBulkString, Bulk: op.Key},
			off,
		)
	}
	return protocol.ArrayOf(
		protocol.BulkOf("OP"),
		protocol.BulkOf("set"),
		protocol.Value{Kind: protocol.KindBulkString, Bulk: op.Key},
		protocol.Value{Kind: protocol.KindBulkString, Bulk: op.Value},
		off,
	)
}

func encodeOps(ops []replication.Op) protocol.Value {
	out := make([]protocol.Value, 0, len(ops))
	for _, op := range ops {
		out = append(out, encodeOp(op))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

// trackReplica 登记一条 PSYNC 建链的副本连接（初始 ACK 为 0），返回注销函数。
func (h *replHandler) trackReplica(conn net.Conn) func() {
	h.mu.Lock()
	if h.replicas == nil {
		h.replicas = make(map[net.Conn]int64)
	}
	h.replicas[conn] = 0
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		delete(h.replicas, conn)
		h.mu.Unlock()
	}
}

// replconf 处理副本挂载与 ACK 上报：LISTENING-PORT 仅校验、CAPA 接受后忽略、
// ACK 记录到建链副本表（未建链连接的 ACK 回 OK 但不计数）、GETACK * 回最新 offset。
// 真机口径（redis 7.2.6 实测）：子命令参数个数不对与未知子命令一律 ERR syntax error。
func (h *replHandler) replconf(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'replconf' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "LISTENING-PORT":
		if len(args) != 2 {
			return errValueStr("ERR syntax error")
		}
		portStr, _ := argString(args[1])
		port, err := strconv.Atoi(portStr)
		if err != nil || port <= 0 || port > 65535 {
			return errValueStr("ERR value is not an integer or out of range")
		}
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "CAPA":
		if len(args) < 2 {
			return errValueStr("ERR syntax error")
		}
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "ACK":
		if len(args) != 2 {
			return errValueStr("ERR syntax error")
		}
		offStr, _ := argString(args[1])
		off, err := strconv.ParseInt(offStr, 10, 64)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
		conn, ok := network.ConnFromContext(ctx)
		if !ok {
			return errValueStr("ERR REPLCONF ACK requires a connection")
		}
		h.mu.Lock()
		if _, tracked := h.replicas[conn]; tracked && off > h.replicas[conn] {
			h.replicas[conn] = off
		}
		h.mu.Unlock()
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "GETACK":
		if len(args) != 2 {
			return errValueStr("ERR syntax error")
		}
		star, _ := argString(args[1])
		if star != "*" {
			return errValueStr("ERR syntax error")
		}
		return protocol.ArrayOf(
			protocol.BulkOf("REPLCONF"),
			protocol.BulkOf("ACK"),
			protocol.BulkOf(strconv.FormatInt(h.hub.Backlog().Latest(), 10)),
		)
	default:
		return errValueStr("ERR syntax error")
	}
}

// ackedCount 返回 ACK 已达 target 的建链副本数。
func (h *replHandler) ackedCount(target int64) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, off := range h.replicas {
		if off >= target {
			n++
		}
	}
	return n
}

// broadcastGetack 向全部建链副本发 REPLCONF GETACK，逐连接起 goroutine
// 写（副本应答 ACK；写阻塞不拖住 WAIT）。
func (h *replHandler) broadcastGetack() {
	h.mu.Lock()
	conns := make([]net.Conn, 0, len(h.replicas))
	for c := range h.replicas {
		conns = append(conns, c)
	}
	h.mu.Unlock()
	frame := protocol.ArrayOf(
		protocol.BulkOf("REPLCONF"),
		protocol.BulkOf("GETACK"),
		protocol.BulkOf("*"),
	).Append(nil)
	for _, c := range conns {
		go func(c net.Conn) {
			_, _ = c.Write(frame)
		}(c)
	}
}

// wait 阻塞到 num 个副本 ACK 到当前最新 offset 或超时，返回实际达标数。
// 无副本时按真机语义阻塞满超时后回 0；num 为 0 直接回 0。
func (h *replHandler) wait(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'wait' command")
	}
	numStr, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	num, err := strconv.Atoi(numStr)
	if err != nil || num < 0 {
		return errValueStr("ERR value is not an integer or out of range")
	}
	toStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	timeoutMs, err := strconv.Atoi(toStr)
	if err != nil || timeoutMs < 0 {
		return errValueStr("ERR value is not an integer or out of range")
	}
	if num == 0 {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	target := h.hub.Backlog().Latest()
	if n := h.ackedCount(target); n >= num {
		return protocol.Value{Kind: protocol.KindInteger, I: int64(n)}
	}
	// WAIT 阻塞期间每 100ms 重发一次 GETACK：副本可能先回旧 offset 的 ACK
	//（目标 OP 还在路上），单次广播会漏醒——周期重发与真机语义一致。
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for {
		h.broadcastGetack()
		end := time.Now().Add(100 * time.Millisecond)
		if end.After(deadline) {
			end = deadline
		}
		for time.Now().Before(end) {
			time.Sleep(10 * time.Millisecond)
			if n := h.ackedCount(target); n >= num {
				return protocol.Value{Kind: protocol.KindInteger, I: int64(n)}
			}
		}
		if !time.Now().Before(deadline) {
			return protocol.Value{Kind: protocol.KindInteger, I: int64(h.ackedCount(target))}
		}
	}
}

// failover 复制级切换：无副本时明确报错；静态拓扑下协同晋升暂不支持。
func (h *replHandler) failover(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'failover' command")
	}
	if h.hub.SubCount() == 0 {
		return errValueStr("ERR FAILOVER requires connected replicas.")
	}
	return errValueStr("ERR FAILOVER coordinated promotion not supported in static topology")
}
