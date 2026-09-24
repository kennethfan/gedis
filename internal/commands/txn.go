package commands

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
)

// TxnRegistry 按连接维护 MULTI 会话与 WATCH 状态：排队、EXEC 回放、CAS 中止、断开清理。
// 排队期做存在性与参数个数检查（错了立即报错并污染会话，EXEC 时 EXECABORT）；
// 类型等错误在回放期由各 handler 报出，落进结果数组对应位置。
// WATCH 脏标记复用复制 Hub：WATCH 时记 backlog 水位，EXEC 前同步追补水位之后的操作，
// 命中 watched key 即中止（回 *-1）；水位已滑出 backlog 时保守全脏。
type TxnRegistry struct {
	mu       sync.Mutex
	sessions map[net.Conn]*txnSession
	execMu   sync.Mutex
	router   *network.Router
	hub      *replication.Hub
}

type txnSession struct {
	queue   []protocol.Value
	dirty   bool
	inMulti bool
	watched map[string]struct{}
	lastOff int64
}

type txnReplayKey struct{}

// CtxWithTxnReplay 给 EXEC 回放用的 ctx 打标记，命中时排队拦截直接放行。
func CtxWithTxnReplay(ctx context.Context) context.Context {
	return context.WithValue(ctx, txnReplayKey{}, true)
}

// RegisterTxn 注册 MULTI/EXEC/DISCARD/WATCH/UNWATCH 并挂载排队拦截；返回 registry 由调用方接断开清理。
func RegisterTxn(r *network.Router, hub *replication.Hub) *TxnRegistry {
	reg := &TxnRegistry{sessions: make(map[net.Conn]*txnSession), router: r, hub: hub}
	r.Register("MULTI", reg.handleMulti)
	r.Register("EXEC", reg.handleExec)
	r.Register("DISCARD", reg.handleDiscard)
	r.Register("WATCH", reg.handleWatch)
	r.Register("UNWATCH", reg.handleUnwatch)
	r.SetIntercept(reg.intercept)
	return reg
}

// ConnClosed 丢弃断开连接的会话（server 侧 OnConnClose 接线，watch 随连接释放）。
func (reg *TxnRegistry) ConnClosed(conn net.Conn) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	delete(reg.sessions, conn)
}

func (reg *TxnRegistry) intercept(ctx context.Context, cmd protocol.Value) (protocol.Value, bool) {
	if ctx.Value(txnReplayKey{}) != nil {
		return protocol.Value{}, false
	}
	if len(cmd.Elems) == 0 || cmd.Elems[0].Kind != protocol.KindBulkString {
		return protocol.Value{}, false
	}
	name := strings.ToUpper(string(cmd.Elems[0].Bulk))
	if name == "MULTI" || name == "EXEC" || name == "DISCARD" || name == "WATCH" {
		return protocol.Value{}, false
	}
	conn, ok := network.ConnFromContext(ctx)
	if !ok {
		return protocol.Value{}, false
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	sess, open := reg.sessions[conn]
	if !open {
		return protocol.Value{}, false
	}
	if name == "UNWATCH" && !sess.inMulti {
		return protocol.Value{}, false
	}
	if !reg.router.Has(name) {
		sess.dirty = true
		return network.UnknownCommandReply(cmd), true
	}
	if !checkArity(commandArity[name], len(cmd.Elems)) {
		sess.dirty = true
		return errValueStr(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(name))), true
	}
	sess.queue = append(sess.queue, cmd)
	return protocol.Value{Kind: protocol.KindSimpleString, S: "QUEUED"}, true
}

func (reg *TxnRegistry) connOf(ctx context.Context) (net.Conn, *protocol.Value) {
	conn, ok := network.ConnFromContext(ctx)
	if !ok {
		v := errValueStr("ERR MULTI/EXEC/DISCARD/WATCH/UNWATCH can only be used in client context")
		return nil, &v
	}
	return conn, nil
}

func (reg *TxnRegistry) latest() int64 {
	if reg.hub == nil {
		return 0
	}
	return reg.hub.Backlog().Latest()
}

// catchupDirty 比对水位之后的操作，命中 watched 即脏；水位滑出 backlog 时保守全脏。
func (reg *TxnRegistry) catchupDirty(sess *txnSession) bool {
	if len(sess.watched) == 0 || reg.hub == nil {
		return false
	}
	missed, err := reg.hub.Backlog().Since(sess.lastOff)
	if err != nil {
		return true
	}
	for _, op := range missed {
		if _, ok := sess.watched[RawToUserKey(string(op.Key))]; ok {
			return true
		}
	}
	return false
}

func (reg *TxnRegistry) handleMulti(ctx context.Context, _ []protocol.Value) protocol.Value {
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if sess, open := reg.sessions[conn]; open && sess.inMulti {
		return errValueStr("ERR MULTI calls can not be nested")
	}
	sess, open := reg.sessions[conn]
	if !open {
		sess = &txnSession{}
		reg.sessions[conn] = sess
	}
	sess.inMulti = true
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (reg *TxnRegistry) handleDiscard(ctx context.Context, _ []protocol.Value) protocol.Value {
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	sess, open := reg.sessions[conn]
	if !open || !sess.inMulti {
		return errValueStr("ERR DISCARD without MULTI")
	}
	delete(reg.sessions, conn)
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (reg *TxnRegistry) handleExec(ctx context.Context, _ []protocol.Value) protocol.Value {
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	sess, open := reg.sessions[conn]
	if !open || !sess.inMulti {
		reg.mu.Unlock()
		return errValueStr("ERR EXEC without MULTI")
	}
	delete(reg.sessions, conn)
	queue, dirty := sess.queue, sess.dirty
	reg.mu.Unlock()
	if dirty {
		return errValueStr("EXECABORT Transaction discarded because of previous errors.")
	}
	reg.execMu.Lock()
	defer reg.execMu.Unlock()
	if reg.catchupDirty(sess) {
		return protocol.Value{Kind: protocol.KindArray}
	}
	if len(queue) == 0 {
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	}
	replayCtx := CtxWithTxnReplay(ctx)
	results := make([]protocol.Value, 0, len(queue))
	for _, q := range queue {
		results = append(results, reg.router.Dispatch(replayCtx, q))
	}
	return protocol.ArrayOf(results...)
}

func (reg *TxnRegistry) handleWatch(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'watch' command")
	}
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if sess, open := reg.sessions[conn]; open && sess.inMulti {
		return errValueStr("ERR WATCH inside MULTI is not allowed")
	}
	sess, open := reg.sessions[conn]
	if !open {
		sess = &txnSession{}
		reg.sessions[conn] = sess
	}
	if len(sess.watched) == 0 {
		sess.lastOff = reg.latest()
	}
	if sess.watched == nil {
		sess.watched = make(map[string]struct{})
	}
	for _, a := range args {
		sess.watched[string(a.Bulk)] = struct{}{}
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (reg *TxnRegistry) handleUnwatch(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'unwatch' command")
	}
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if sess, open := reg.sessions[conn]; open {
		sess.watched = nil
		if len(sess.queue) == 0 && !sess.inMulti {
			delete(reg.sessions, conn)
		}
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}
