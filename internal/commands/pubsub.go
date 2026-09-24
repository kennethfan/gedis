package commands

import (
	"context"
	"net"
	"strings"
	"sync"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// PubSubRegistry 按连接维护 Pub/Sub 订阅：订阅集合（保序）、sender goroutine、断开清理。
// 线级语义与 Redis 7.2 对齐（2026-09 探针，redis 7.2.6）：
//   - 订阅确认/退订确认为 [kind channel count] 三元数组；空 UNSUBSCRIBE 按订阅逆序逐个退订，
//     零订阅时回单个 [unsubscribe nil 0]；重复订阅重发确认且计数不变。
//   - 推送为 [message channel payload]；PUBLISH 返回接收者数；订阅者队列 buffer 64 +
//     阻塞发布（慢订阅者反压发布者；与 Redis 输出缓冲杀慢消费者不同，v1 接受此差异）。
//   - 订阅态（n>0）仅放行 SUBSCRIBE/UNSUBSCRIBE/PING/QUIT/RESET（含 P(S)SUBSCRIBE 透传），
//     其余一律 "Can't execute ..."（大小写：命令名小写化）；退订至零即退出订阅态。
//   - 订阅态 PING 回 [pong data?] 二元数组（无参 data 为空串；双参报 ping-arity 错）。
//   - MULTI 内 SUBSCRIBE 照常 QUEUED，EXEC 结果数组只装首个确认、其余直写（真机行为）。
type PubSubRegistry struct {
	mu       sync.Mutex
	sessions map[net.Conn]*pubsubSession
}

type pubsubSession struct {
	subs   map[string]struct{}
	order  []string
	ch     chan protocol.Value
	sender bool
	closed bool
}

// subModeAllowed 订阅态放行的命令（含未实现的 P(S)SUBSCRIBE/QUIT/RESET：透传给正常分发）。
var subModeAllowed = map[string]bool{
	"SUBSCRIBE": true, "UNSUBSCRIBE": true, "PSUBSCRIBE": true, "PUNSUBSCRIBE": true,
	"PING": true, "QUIT": true, "RESET": true,
}

// RegisterPubSub 注册 SUBSCRIBE/UNSUBSCRIBE/PUBLISH，包裹订阅态 PING 并追挂订阅态拦截。
func RegisterPubSub(r *network.Router) *PubSubRegistry {
	reg := &PubSubRegistry{sessions: make(map[net.Conn]*pubsubSession)}
	r.Register("SUBSCRIBE", reg.handleSubscribe)
	r.Register("UNSUBSCRIBE", reg.handleUnsubscribe)
	r.Register("PUBLISH", reg.handlePublish)
	if pingH, ok := r.Handler("PING"); ok {
		r.Register("PING", reg.wrapPing(pingH))
	}
	r.ChainIntercept(reg.intercept)
	return reg
}

// ConnClosed 丢弃断开连接的会话并停 sender（server 侧 OnConnClose 接线）。
func (reg *PubSubRegistry) ConnClosed(conn net.Conn) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if sess, ok := reg.sessions[conn]; ok {
		delete(reg.sessions, conn)
		reg.stopSenderLocked(sess)
	}
}

// intercept 订阅态拦截：EXEC 回放与非订阅连接直接放行；订阅态下非放行命令报
// "ERR Can't execute '<小写命令>': only ..."（与 Redis 7.2 文案逐字节一致）。
func (reg *PubSubRegistry) intercept(ctx context.Context, cmd protocol.Value) (protocol.Value, bool) {
	if ctx.Value(txnReplayKey{}) != nil {
		return protocol.Value{}, false
	}
	if len(cmd.Elems) == 0 || cmd.Elems[0].Kind != protocol.KindBulkString {
		return protocol.Value{}, false
	}
	conn, ok := network.ConnFromContext(ctx)
	if !ok {
		return protocol.Value{}, false
	}
	name := strings.ToUpper(string(cmd.Elems[0].Bulk))
	if subModeAllowed[name] {
		return protocol.Value{}, false
	}
	reg.mu.Lock()
	sess, has := reg.sessions[conn]
	n := 0
	if has {
		n = len(sess.subs)
	}
	reg.mu.Unlock()
	if n == 0 {
		return protocol.Value{}, false
	}
	return errValueStr("ERR Can't execute '" + strings.ToLower(name) +
		"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"), true
}

func (reg *PubSubRegistry) sessionLocked(conn net.Conn) *pubsubSession {
	sess, ok := reg.sessions[conn]
	if !ok {
		sess = &pubsubSession{subs: make(map[string]struct{})}
		reg.sessions[conn] = sess
	}
	return sess
}

func (reg *PubSubRegistry) stopSenderLocked(sess *pubsubSession) {
	if sess.sender && !sess.closed {
		sess.closed = true
		close(sess.ch)
	}
}

func (reg *PubSubRegistry) removeLocked(sess *pubsubSession, channel string) {
	delete(sess.subs, channel)
	for i, c := range sess.order {
		if c == channel {
			sess.order = append(sess.order[:i], sess.order[i+1:]...)
			break
		}
	}
	if len(sess.subs) == 0 {
		reg.stopSenderLocked(sess)
	}
}

func confirm(kind, channel string, count int64) protocol.Value {
	return protocol.ArrayOf(
		protocol.BulkOf(kind),
		protocol.BulkOf(channel),
		protocol.Value{Kind: protocol.KindInteger, I: count},
	)
}

func (reg *PubSubRegistry) writeDirect(conn net.Conn, v protocol.Value) {
	_, _ = conn.Write(v.Append(nil))
}

func (reg *PubSubRegistry) connOf(ctx context.Context) (net.Conn, *protocol.Value) {
	conn, ok := network.ConnFromContext(ctx)
	if !ok {
		v := errValueStr("ERR SUBSCRIBE/UNSUBSCRIBE/PUBLISH can only be used in client context")
		return nil, &v
	}
	return conn, nil
}

func (reg *PubSubRegistry) handleSubscribe(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'subscribe' command")
	}
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	sess := reg.sessionLocked(conn)
	confs := make([]protocol.Value, 0, len(args))
	for _, a := range args {
		channel := string(a.Bulk)
		if len(sess.subs) == 0 && !sess.sender {
			sess.ch = make(chan protocol.Value, 64)
			sess.sender = true
			go runSender(conn, sess.ch)
		}
		if _, dup := sess.subs[channel]; !dup {
			sess.subs[channel] = struct{}{}
			sess.order = append(sess.order, channel)
		}
		confs = append(confs, confirm("subscribe", channel, int64(len(sess.subs))))
	}
	reg.mu.Unlock()
	return reg.emitConfs(ctx, conn, confs)
}

func (reg *PubSubRegistry) handleUnsubscribe(ctx context.Context, args []protocol.Value) protocol.Value {
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	sess := reg.sessionLocked(conn)
	var targets []string
	if len(args) == 0 {
		targets = make([]string, len(sess.order))
		for i, c := range sess.order {
			targets[len(sess.order)-1-i] = c
		}
	} else {
		targets = make([]string, 0, len(args))
		for _, a := range args {
			targets = append(targets, string(a.Bulk))
		}
	}
	confs := make([]protocol.Value, 0, len(targets))
	if len(targets) == 0 {
		confs = append(confs, protocol.ArrayOf(
			protocol.BulkOf("unsubscribe"),
			protocol.Value{Kind: protocol.KindBulkString},
			protocol.Value{Kind: protocol.KindInteger, I: 0},
		))
	}
	for _, channel := range targets {
		reg.removeLocked(sess, channel)
		confs = append(confs, protocol.ArrayOf(
			protocol.BulkOf("unsubscribe"),
			protocol.BulkOf(channel),
			protocol.Value{Kind: protocol.KindInteger, I: int64(len(sess.subs))},
		))
	}
	reg.mu.Unlock()
	return reg.emitConfs(ctx, conn, confs)
}

func (reg *PubSubRegistry) handlePublish(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'publish' command")
	}
	channel := string(args[0].Bulk)
	msg := protocol.ArrayOf(
		protocol.BulkOf("message"),
		protocol.BulkOf(channel),
		args[1],
	)
	reg.mu.Lock()
	var targets []chan protocol.Value
	for _, sess := range reg.sessions {
		if _, ok := sess.subs[channel]; ok && sess.sender && !sess.closed {
			targets = append(targets, sess.ch)
		}
	}
	reg.mu.Unlock()
	for _, ch := range targets {
		ch <- msg
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(targets))}
}

// emitConfs 按执行路径决定确认数组形状：EXEC 回放返回 FIRST、直写其余；
// 普通路径直写前 n-1、返回 LAST。与 Redis 7.2 线序一致（探针：多频道 SUB 进
// MULTI 时 EXEC 结果只装首个确认、其余带外推送）。
func (reg *PubSubRegistry) emitConfs(ctx context.Context, conn net.Conn, confs []protocol.Value) protocol.Value {
	if ctx.Value(txnReplayKey{}) != nil {
		for _, c := range confs[1:] {
			reg.writeDirect(conn, c)
		}
		return confs[0]
	}
	for _, c := range confs[:len(confs)-1] {
		reg.writeDirect(conn, c)
	}
	return confs[len(confs)-1]
}

func (reg *PubSubRegistry) wrapPing(next network.Handler) network.Handler {
	return func(ctx context.Context, args []protocol.Value) protocol.Value {
		if len(args) > 1 {
			return errValueStr("ERR wrong number of arguments for 'ping' command")
		}
		conn, ok := network.ConnFromContext(ctx)
		if !ok {
			return next(ctx, args)
		}
		reg.mu.Lock()
		sess, has := reg.sessions[conn]
		n := 0
		if has {
			n = len(sess.subs)
		}
		reg.mu.Unlock()
		if n == 0 {
			return next(ctx, args)
		}
		elems := []protocol.Value{protocol.BulkOf("pong")}
		if len(args) == 0 {
			elems = append(elems, protocol.BulkOf(""))
		} else {
			elems = append(elems, args...)
		}
		return protocol.ArrayOf(elems...)
	}
}

func runSender(conn net.Conn, ch <-chan protocol.Value) {
	for msg := range ch {
		if _, err := conn.Write(msg.Append(nil)); err != nil {
			return
		}
	}
}
