package commands

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
)

// RegisterReplication 注册 PSYNC（主库侧）与 REPLICAOF；REPLICAOF 启停
// 复本客户端并切换只读模式与 Stats 角色。
func RegisterReplication(r *network.Router, kv KV, stats *network.Stats, hub *replication.Hub) {
	h := &replHandler{router: r, kv: kv, stats: stats, hub: hub}
	r.Register("PSYNC", h.psync)
	r.Register("REPLICAOF", h.replicaof)
}

type replHandler struct {
	router *network.Router
	kv     KV
	stats  *network.Stats
	hub    *replication.Hub
	client *replication.Client
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
			go h.sendLoop(conn, id, ch)
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
	go h.sendLoop(conn, id, ch)
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
func (h *replHandler) sendLoop(conn net.Conn, id int64, ch <-chan []replication.Op) {
	defer func() {
		h.hub.Unsubscribe(id)
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
