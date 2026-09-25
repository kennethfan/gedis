package commands

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// Cluster 接入（M7 最小行为集）：
//   - 接入点：Router.ChainIntercept 挂 intercept（Txn 之后，排队命令已被 QUEUED
//     吞掉，intercept 只见非事务命令；EXEC 回放 ctx 带 txnReplay 标记豁免，
//     队列整体由 TxnRegistry.PreExec 预扫）。
//   - Lua 直调 Handler 天然绕过（#39：脚本内命令不额外约束；EVAL 入口
//     仍经 intercept 做 KEYS 同槽校验）。
//   - ASKING 态：AskRegistry（map[net.Conn]bool），ConnClosed 供 server 侧
//     OnConnClose 接线。
//   - 静态拓扑：无迁移态。asked+未持有+本地有 key→放行（对标 ask.fixture §4）；
//     asked+未持有+本地无 key→ASK（对标 §2 源端）；未 asked+未持有→MOVED。
//
// disabled 对标真机 standalone：CLUSTER 返回 disabled 错误；ASKING 压根不注册，
// 路由回 unknown-command 错误（7.2.6 实测，不注册即原文 `unknown command 'asking'`）。
const errClusterDisabled = "ERR This instance has cluster support disabled"

// AskRegistry 存单连接一次性 ASKING 标志；ASKING 命令置位，下条命令消费后清除。
type AskRegistry struct {
	mu sync.Mutex
	m  map[net.Conn]bool
}

func NewAskRegistry() *AskRegistry {
	return &AskRegistry{m: make(map[net.Conn]bool)}
}

// ConnClosed 丢弃断开连接的标志（server 侧 OnConnClose 接线）。
func (a *AskRegistry) ConnClosed(conn net.Conn) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.m, conn)
}

// Set 置位本连接 ASKING；无连接上下文（单测直调）返回 false。
func (a *AskRegistry) Set(ctx context.Context) bool {
	conn, ok := network.ConnFromContext(ctx)
	if !ok {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.m[conn] = true
	return true
}

// Consume 取出并清除本连接标志；无连接上下文返回 false。
func (a *AskRegistry) Consume(ctx context.Context) bool {
	conn, ok := network.ConnFromContext(ctx)
	if !ok {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	set := a.m[conn]
	delete(a.m, conn)
	return set
}

// RegisterCluster 注册 CLUSTER（常驻）/ ASKING（仅 enabled）并挂载重定向 intercept。
// topo 为 nil 视为集群关闭。返回 handler 供 main.go 挂 TxnRegistry.PreExec。
func RegisterCluster(r *network.Router, kv KV, topo *cluster.Topology, asking *AskRegistry) *clusterHandler {
	c := &clusterHandler{kv: kv, topo: topo, askReg: asking}
	r.Register("CLUSTER", c.cluster)
	if c.enabled() {
		r.Register("ASKING", c.asking)
	}
	r.ChainIntercept(c.intercept)
	return c
}

type clusterHandler struct {
	kv     KV
	topo   *cluster.Topology
	askReg *AskRegistry
}

func (c *clusterHandler) enabled() bool {
	return c.topo != nil && c.topo.Enabled
}

func clusterArgErr(sub string) protocol.Value {
	return errValueStr(fmt.Sprintf("ERR wrong number of arguments for 'cluster|%s' command", strings.ToLower(sub)))
}

func (c *clusterHandler) cluster(ctx context.Context, args []protocol.Value) protocol.Value {
	if !c.enabled() {
		return errValueStr(errClusterDisabled)
	}
	if len(args) == 0 {
		return errValueStr("ERR wrong number of arguments for 'cluster' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	rest := args[1:]
	switch strings.ToUpper(sub) {
	case "KEYSLOT":
		if len(rest) != 1 {
			return clusterArgErr(sub)
		}
		key, ok := argString(rest[0])
		if !ok {
			return errValueStr("ERR invalid key")
		}
		return protocol.Value{Kind: protocol.KindInteger, I: int64(cluster.Slot(key))}
	case "MYID":
		if len(rest) != 0 {
			return clusterArgErr(sub)
		}
		return protocol.BulkOf(c.topo.SelfID())
	case "SLOTS":
		if len(rest) != 0 {
			return clusterArgErr(sub)
		}
		return c.slots()
	case "SHARDS":
		if len(rest) != 0 {
			return clusterArgErr(sub)
		}
		return c.shards()
	case "INFO":
		if len(rest) != 0 {
			return clusterArgErr(sub)
		}
		return c.info()
	case "NODES":
		if len(rest) != 0 {
			return clusterArgErr(sub)
		}
		return c.nodes()
	case "COUNTKEYSINSLOT":
		return c.countKeysInSlot(rest)
	case "GETKEYSINSLOT":
		return c.getKeysInSlot(ctx, rest)
	case "MEET":
		return errValueStr("ERR Static cluster topology does not support CLUSTER MEET")
	case "FORGET":
		return c.forget(rest)
	case "REPLICATE":
		return c.replicate(rest)
	case "FAILOVER":
		return c.failover(rest)
	case "RESET":
		return c.reset(rest)
	case "FLUSHSLOTS":
		if len(rest) != 0 {
			return clusterArgErr(sub)
		}
		return errValueStr("ERR Static cluster topology does not support CLUSTER FLUSHSLOTS")
	case "ADDSLOTS", "DELSLOTS":
		return errValueStr("ERR Please use SETSLOT only to update slots in the cluster nodes.")
	case "ADDSLOTSRANGE", "DELSLOTSRANGE":
		return errValueStr("ERR Please use SETSLOT only to update slots in the cluster nodes.")
	case "SETSLOT":
		return c.setslot(rest)
	case "SET-CONFIG-EPOCH":
		return errValueStr("ERR Static cluster topology does not support CLUSTER SET-CONFIG-EPOCH")
	case "SAVECONFIG":
		if len(rest) != 0 {
			return clusterArgErr(sub)
		}
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "MYSHARDID", "MYPRIMARYID", "LINKS":
		return errValueStr("ERR Static cluster topology does not support CLUSTER " + strings.ToUpper(sub))
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s'. Try CLUSTER HELP.", string(args[0].Bulk)))
	}
}

func (c *clusterHandler) asking(ctx context.Context, args []protocol.Value) protocol.Value {
	if !c.enabled() {
		return errValueStr(errClusterDisabled)
	}
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'asking' command")
	}
	c.askReg.Set(ctx)
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

// slots 按真机形状输出：[start end [ip port id] …]，同节点连续段合并。
func (c *clusterHandler) slots() protocol.Value {
	var out []protocol.Value
	for _, n := range c.topo.Nodes() {
		ranges := merged(n.Ranges)
		for _, r := range ranges {
			entry := []protocol.Value{
				{Kind: protocol.KindInteger, I: int64(r[0])},
				{Kind: protocol.KindInteger, I: int64(r[1])},
				nodeAddrTriple(n),
			}
			out = append(out, protocol.ArrayOf(entry...))
		}
	}
	return protocol.ArrayOf(out...)
}

func nodeAddrTriple(n cluster.TopoNode) protocol.Value {
	host, port := splitHostPort(n.Addr)
	return protocol.ArrayOf(
		protocol.BulkOf(host),
		protocol.Value{Kind: protocol.KindInteger, I: int64(port)},
		protocol.BulkOf(n.ID),
	)
}

// shards 按真机形状输出：[{slots [s e …] nodes [{id port ip endpoint role …}]}…]。
func (c *clusterHandler) shards() protocol.Value {
	var out []protocol.Value
	for _, n := range c.topo.Nodes() {
		var slots []protocol.Value
		for _, r := range n.Ranges {
			slots = append(slots,
				protocol.Value{Kind: protocol.KindInteger, I: int64(r[0])},
				protocol.Value{Kind: protocol.KindInteger, I: int64(r[1])},
			)
		}
		host, port := splitHostPort(n.Addr)
		nodeMap := protocol.Value{Kind: protocol.KindMap, Pairs: []protocol.Pair{
			{K: protocol.BulkOf("id"), V: protocol.BulkOf(n.ID)},
			{K: protocol.BulkOf("port"), V: protocol.Value{Kind: protocol.KindInteger, I: int64(port)}},
			{K: protocol.BulkOf("ip"), V: protocol.BulkOf(host)},
			{K: protocol.BulkOf("endpoint"), V: protocol.BulkOf(host)},
			{K: protocol.BulkOf("role"), V: protocol.BulkOf("master")},
			{K: protocol.BulkOf("replication-offset"), V: protocol.Value{Kind: protocol.KindInteger, I: 0}},
			{K: protocol.BulkOf("health"), V: protocol.BulkOf("online")},
		}}
		shard := protocol.Value{Kind: protocol.KindMap, Pairs: []protocol.Pair{
			{K: protocol.BulkOf("slots"), V: protocol.ArrayOf(slots...)},
			{K: protocol.BulkOf("nodes"), V: protocol.ArrayOf(nodeMap)},
		}}
		out = append(out, shard)
	}
	return protocol.ArrayOf(out...)
}

// info 输出稳定子集（计数器/epoch 天然静态，见 fixtures/info.redis 注记）。
func (c *clusterHandler) info() protocol.Value {
	assigned := c.topo.AssignedCount()
	var sb strings.Builder
	fmt.Fprintf(&sb, "cluster_state:ok\r\n")
	fmt.Fprintf(&sb, "cluster_slots_assigned:%d\r\n", assigned)
	fmt.Fprintf(&sb, "cluster_slots_ok:%d\r\n", assigned)
	fmt.Fprintf(&sb, "cluster_slots_pfail:0\r\n")
	fmt.Fprintf(&sb, "cluster_slots_fail:0\r\n")
	fmt.Fprintf(&sb, "cluster_known_nodes:%d\r\n", len(c.topo.Nodes()))
	fmt.Fprintf(&sb, "cluster_size:%d\r\n", len(c.topo.Nodes()))
	fmt.Fprintf(&sb, "cluster_current_epoch:1\r\n")
	fmt.Fprintf(&sb, "cluster_my_epoch:1\r\n")
	return protocol.BulkOf(sb.String())
}

// nodes 输出静态 NODES 行：self 带 myself,master，cport=port+10000。
func (c *clusterHandler) nodes() protocol.Value {
	var sb strings.Builder
	selfID := c.topo.SelfID()
	for _, n := range c.topo.Nodes() {
		_, port := splitHostPort(n.Addr)
		flags := "master"
		master := "-"
		if n.ID == selfID {
			flags = "myself,master"
		}
		fmt.Fprintf(&sb, "%s %s@%d %s %s 0 0 1 connected",
			n.ID, n.Addr, port+10000, flags, master)
		for _, r := range n.Ranges {
			if r[0] == r[1] {
				fmt.Fprintf(&sb, " %d", r[0])
			} else {
				fmt.Fprintf(&sb, " %d-%d", r[0], r[1])
			}
		}
		sb.WriteString("\r\n")
	}
	return protocol.BulkOf(sb.String())
}

func parseSlotArg(s string) (int, bool) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n >= cluster.NumSlots {
		return 0, false
	}
	return n, true
}

func (c *clusterHandler) countKeysInSlot(rest []protocol.Value) protocol.Value {
	if len(rest) != 2 {
		return clusterArgErr("COUNTKEYSINSLOT")
	}
	slotStr, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR Invalid or out of range slot")
	}
	slot, valid := parseSlotArg(slotStr)
	if !valid {
		return errValueStr("ERR Invalid or out of range slot")
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(c.keysInSlotNames(context.Background(), slot, -1)))}
}

func (c *clusterHandler) getKeysInSlot(ctx context.Context, rest []protocol.Value) protocol.Value {
	if len(rest) != 2 {
		return clusterArgErr("GETKEYSINSLOT")
	}
	slotStr, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR Invalid or out of range slot")
	}
	slot, valid := parseSlotArg(slotStr)
	if !valid {
		return errValueStr("ERR Invalid or out of range slot")
	}
	countStr, ok := argString(rest[1])
	if !ok {
		return errValueStr("ERR Invalid COUNT")
	}
	count, err := strconv.Atoi(countStr)
	if err != nil || count < 0 {
		return errValueStr("ERR Invalid COUNT")
	}
	names := c.keysInSlotNames(ctx, slot, count)
	out := make([]protocol.Value, 0, len(names))
	for _, n := range names {
		out = append(out, protocol.BulkOf(n))
	}
	return protocol.ArrayOf(out...)
}

// forget 对标真机：忘自己拒绝；忘他人静态不支持。
func (c *clusterHandler) forget(rest []protocol.Value) protocol.Value {
	if len(rest) != 1 {
		return clusterArgErr("FORGET")
	}
	id, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	if id == c.topo.SelfID() {
		return errValueStr("ERR I tried hard but I can't forget myself...")
	}
	return errValueStr("ERR Static cluster topology does not support CLUSTER FORGET")
}

// replicate 对标真机：复制自己拒绝；静态无 replica 位。
func (c *clusterHandler) replicate(rest []protocol.Value) protocol.Value {
	if len(rest) != 1 {
		return clusterArgErr("REPLICATE")
	}
	id, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	if id == c.topo.SelfID() {
		return errValueStr("ERR Can't replicate myself")
	}
	return errValueStr("ERR Static cluster topology does not support CLUSTER REPLICATE")
}

// failover 静态全员 master：master 发起即拒（真机措辞）；TAKEOVER 同理。
func (c *clusterHandler) failover(rest []protocol.Value) protocol.Value {
	if len(rest) > 1 {
		return errValueStr("ERR syntax error")
	}
	if len(rest) == 1 {
		opt, _ := argString(rest[0])
		if strings.ToUpper(opt) != "FORCE" && strings.ToUpper(opt) != "TAKEOVER" {
			return errValueStr("ERR syntax error")
		}
	}
	return errValueStr("ERR You should send CLUSTER FAILOVER to a replica")
}

// reset 参数校验对标真机（HARD/SOFT），执行面静态拒绝。
func (c *clusterHandler) reset(rest []protocol.Value) protocol.Value {
	if len(rest) > 1 {
		return clusterArgErr("RESET")
	}
	if len(rest) == 1 {
		opt, _ := argString(rest[0])
		if strings.ToUpper(opt) != "HARD" && strings.ToUpper(opt) != "SOFT" {
			return errValueStr("ERR syntax error")
		}
	}
	return errValueStr("ERR Static cluster topology does not support CLUSTER RESET")
}

// setslot 逐分支对标真机前置校验（owner/busy/self），通过后静态拒绝。
func (c *clusterHandler) setslot(rest []protocol.Value) protocol.Value {
	if len(rest) < 2 {
		return clusterArgErr("SETSLOT")
	}
	slotStr, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR Invalid or out of range slot")
	}
	slot, valid := parseSlotArg(slotStr)
	if !valid {
		return errValueStr("ERR Invalid or out of range slot")
	}
	op, _ := argString(rest[1])
	switch strings.ToUpper(op) {
	case "MIGRATING":
		if len(rest) != 3 {
			return errValueStr("ERR syntax error")
		}
		if !c.topo.Owns(slot) {
			return errValueStr(fmt.Sprintf("ERR I'm not the owner of hash slot %d", slot))
		}
		return errValueStr("ERR Static cluster topology does not support CLUSTER SETSLOT")
	case "IMPORTING":
		if len(rest) != 3 {
			return errValueStr("ERR syntax error")
		}
		if c.topo.Owns(slot) {
			return errValueStr("ERR I'm already the owner of hash slot.")
		}
		return errValueStr("ERR Static cluster topology does not support CLUSTER SETSLOT")
	case "STABLE", "NODE":
		return errValueStr("ERR Static cluster topology does not support CLUSTER SETSLOT")
	default:
		return errValueStr("ERR syntax error")
	}
}

// intercept 是 MOVED/ASK 重定向与 CROSSSLOT 门卫（ChainIntercept 挂载）。
func (c *clusterHandler) intercept(ctx context.Context, cmd protocol.Value) (protocol.Value, bool) {
	if !c.enabled() {
		return protocol.Value{}, false
	}
	if ctx.Value(txnReplayKey{}) != nil {
		return protocol.Value{}, false
	}
	if len(cmd.Elems) == 0 || cmd.Elems[0].Kind != protocol.KindBulkString {
		return protocol.Value{}, false
	}
	name := strings.ToUpper(string(cmd.Elems[0].Bulk))
	if name == "ASKING" {
		return protocol.Value{}, false
	}
	asked := c.askReg.Consume(ctx)
	strArgs := make([]string, 0, len(cmd.Elems)-1)
	for _, a := range cmd.Elems[1:] {
		s, ok := argString(a)
		if !ok {
			return protocol.Value{}, false
		}
		strArgs = append(strArgs, s)
	}
	keys, ok := cluster.KeysOf(name, strArgs)
	if !ok || len(keys) == 0 {
		return protocol.Value{}, false
	}
	slot := cluster.Slot(keys[0])
	for _, k := range keys[1:] {
		if cluster.Slot(k) != slot {
			return errValueStr("CROSSSLOT Keys in request don't hash to the same slot"), true
		}
	}
	if c.topo.Owns(slot) {
		return protocol.Value{}, false
	}
	owner := c.topo.OwnerAddr(slot)
	if owner == "" {
		return errValueStr("CLUSTERDOWN Hash slot not served by this node. Check your cluster configuration."), true
	}
	if asked && c.keyPresent(ctx, keys[0]) {
		return protocol.Value{}, false
	}
	if asked {
		return errValueStr(fmt.Sprintf("ASK %d %s", slot, owner)), true
	}
	return errValueStr(fmt.Sprintf("MOVED %d %s", slot, owner)), true
}

// CheckExec 供 TxnRegistry.PreExec：队列整体预扫，同槽才放行回放。
// 单槽未持有→MOVED（真机 EXEC 路由语义）；asked 标志在 EXEC 路径不消费。
func (c *clusterHandler) CheckExec(_ context.Context, queue []protocol.Value) *protocol.Value {
	if !c.enabled() || len(queue) == 0 {
		return nil
	}
	slot := -1
	for _, q := range queue {
		if len(q.Elems) == 0 || q.Elems[0].Kind != protocol.KindBulkString {
			continue
		}
		name := strings.ToUpper(string(q.Elems[0].Bulk))
		var strArgs []string
		for _, a := range q.Elems[1:] {
			s, ok := argString(a)
			if !ok {
				break
			}
			strArgs = append(strArgs, s)
		}
		keys, ok := cluster.KeysOf(name, strArgs)
		if !ok || len(keys) == 0 {
			continue
		}
		for _, k := range keys {
			s := cluster.Slot(k)
			if slot < 0 {
				slot = s
			} else if s != slot {
				v := errValueStr("CROSSSLOT Keys in request don't hash to the same slot")
				return &v
			}
		}
	}
	if slot >= 0 && !c.topo.Owns(slot) {
		owner := c.topo.OwnerAddr(slot)
		var v protocol.Value
		if owner == "" {
			v = errValueStr("CLUSTERDOWN Hash slot not served by this node. Check your cluster configuration.")
		} else {
			v = errValueStr(fmt.Sprintf("MOVED %d %s", slot, owner))
		}
		return &v
	}
	return nil
}

// keyPresent 跨类型前缀探测 key 存在性（仅 ASKING 命中后的稀有路径调用）。
func (c *clusterHandler) keyPresent(ctx context.Context, key string) bool {
	if c.kv == nil {
		return false
	}
	_, err := lookupKey(ctx, c.kv, key)
	return err == nil
}

// merged 合并连续区间（SLOTS 输出与真机一致：连续同主段只占一项）。
func merged(ranges [][2]int) [][2]int {
	if len(ranges) == 0 {
		return nil
	}
	out := [][2]int{ranges[0]}
	for _, r := range ranges[1:] {
		last := &out[len(out)-1]
		if r[0] <= last[1]+1 {
			if r[1] > last[1] {
				last[1] = r[1]
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

func splitHostPort(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	port, _ := strconv.Atoi(portStr)
	return host, port
}

// keysInSlotNames 返回该槽本地 key（前缀表顺序，确定性；count<0 不限量）。
func (c *clusterHandler) keysInSlotNames(ctx context.Context, slot, count int) []string {
	if c.kv == nil {
		return nil
	}
	names, _, err := allUserKeys(ctx, c.kv)
	if err != nil {
		return nil
	}
	var out []string
	for _, n := range names {
		if cluster.Slot(n) != slot {
			continue
		}
		out = append(out, n)
		if count >= 0 && len(out) >= count {
			break
		}
	}
	return out
}
