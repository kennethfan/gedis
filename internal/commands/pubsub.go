package commands

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/kennethfan/gedis/internal/acl"
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
	subs     map[string]struct{}
	order    []string
	patterns map[string]struct{}
	porder   []string
	shards   map[string]struct{}
	sorder   []string
	ch       chan protocol.Value
	sender   bool
	closed   bool
}

// total 是 subscribe 域计数（普通+pattern，不含 shard）——SUBSCRIBE 系确认用。
func (s *pubsubSession) total() int {
	return len(s.subs) + len(s.patterns)
}

// shardTotal 是 shard 域计数——SSUBSCRIBE 系确认用（真机两域独立）。
func (s *pubsubSession) shardTotal() int {
	return len(s.shards)
}

// activeTotal 决定订阅态（拦截/PING/sender 存活），任一域非零即订阅态。
func (s *pubsubSession) activeTotal() int {
	return len(s.subs) + len(s.patterns) + len(s.shards)
}

// subModeAllowed 订阅态放行的命令（QUIT/RESET 未实现：透传给正常分发）。
var subModeAllowed = map[string]bool{
	"SUBSCRIBE": true, "UNSUBSCRIBE": true, "PSUBSCRIBE": true, "PUNSUBSCRIBE": true,
	"SSUBSCRIBE": true, "SUNSUBSCRIBE": true,
	"PING": true, "QUIT": true, "RESET": true,
}

// RegisterPubSub 注册 SUBSCRIBE/UNSUBSCRIBE/PUBLISH，包裹订阅态 PING 并追挂订阅态拦截。
var pubsubMeta = []acl.Meta{
	{Name: "SUBSCRIBE", Category: "pubsub", Keys: acl.KeySpec{First: -1}},
	{Name: "UNSUBSCRIBE", Category: "pubsub", Keys: acl.KeySpec{First: -1}},
	{Name: "PSUBSCRIBE", Category: "pubsub", Keys: acl.KeySpec{First: -1}},
	{Name: "PUNSUBSCRIBE", Category: "pubsub", Keys: acl.KeySpec{First: -1}},
	{Name: "SSUBSCRIBE", Category: "pubsub", Keys: acl.KeySpec{First: -1}},
	{Name: "SUNSUBSCRIBE", Category: "pubsub", Keys: acl.KeySpec{First: -1}},
	{Name: "PUBLISH", Category: "pubsub", Keys: acl.KeySpec{First: -1}},
	{Name: "SPUBLISH", Category: "pubsub", Keys: acl.KeySpec{First: -1}},
	{Name: "PUBSUB", Category: "pubsub", Keys: acl.KeySpec{First: -1}},
	{Name: "PING", Category: "connection", ReadOnly: true, Keys: acl.KeySpec{First: -1}},
}

// pubsubSubs 已实现的 PUBSUB 子命令（大写）。intercept 用它区分「未知子命令/
// arity 错优先于订阅态拦截」与「合法子命令报 cmd|sub 拦截错」（真机 7.2.6 顺序）。
var pubsubSubs = map[string]bool{
	"CHANNELS": true, "NUMSUB": true, "NUMPAT": true, "HELP": true,
	"SHARDCHANNELS": true, "SHARDNUMSUB": true,
}

func RegisterPubSub(r *network.Router) *PubSubRegistry {
	for _, m := range pubsubMeta {
		acl.RegisterMeta(m)
	}
	reg := &PubSubRegistry{sessions: make(map[net.Conn]*pubsubSession)}
	r.Register("SUBSCRIBE", reg.handleSubscribe)
	r.Register("UNSUBSCRIBE", reg.handleUnsubscribe)
	r.Register("PSUBSCRIBE", reg.handlePsubscribe)
	r.Register("PUNSUBSCRIBE", reg.handlePunsubscribe)
	r.Register("PUBLISH", reg.handlePublish)
	r.Register("SPUBLISH", reg.handleSpublish)
	r.Register("PUBSUB", reg.handlePubSub)
	r.Register("SSUBSCRIBE", reg.handleSsubscribe)
	r.Register("SUNSUBSCRIBE", reg.handleSunsubscribe)
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
// PUBSUB 特判（真机 7.2.6 顺序）：容器 arity 错、未知子命令错优先于拦截——
// 裸 PUBSUB、PUBSUB foo、PUBSUB NUMPAT x 放行给 handler 出各自错误；
// 合法子命令报 "pubsub|<sub小写>" 拦截错。其余容器命令的 |sub 拼接是既有
// 偏差（CONFIG GET 等报 'config'），本任务不扩散，见 ledger。
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
	if name == "PUBSUB" {
		if len(cmd.Elems) < 2 || cmd.Elems[1].Kind != protocol.KindBulkString {
			return protocol.Value{}, false
		}
		sub := strings.ToUpper(string(cmd.Elems[1].Bulk))
		if !pubsubSubs[sub] {
			return protocol.Value{}, false
		}
		if sub == "NUMPAT" || sub == "HELP" {
			if len(cmd.Elems) != 2 {
				return protocol.Value{}, false
			}
		}
	}
	reg.mu.Lock()
	sess, has := reg.sessions[conn]
	n := 0
	if has {
		n = sess.activeTotal()
	}
	reg.mu.Unlock()
	if n == 0 {
		return protocol.Value{}, false
	}
	if name == "PUBSUB" {
		sub := strings.ToLower(string(cmd.Elems[1].Bulk))
		return errValueStr("ERR Can't execute 'pubsub|" + sub +
			"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"), true
	}
	return errValueStr("ERR Can't execute '" + strings.ToLower(name) +
		"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"), true
}

func (reg *PubSubRegistry) sessionLocked(conn net.Conn) *pubsubSession {
	sess, ok := reg.sessions[conn]
	if !ok {
		sess = &pubsubSession{
			subs:     make(map[string]struct{}),
			patterns: make(map[string]struct{}),
			shards:   make(map[string]struct{}),
		}
		reg.sessions[conn] = sess
	}
	return sess
}

func (reg *PubSubRegistry) ensureSenderLocked(conn net.Conn, sess *pubsubSession) {
	if sess.sender && !sess.closed {
		return
	}
	sess.ch = make(chan protocol.Value, 64)
	sess.closed = false
	sess.sender = true
	go runSender(conn, sess.ch)
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
	if sess.activeTotal() == 0 {
		reg.stopSenderLocked(sess)
	}
}

func (reg *PubSubRegistry) removePatternLocked(sess *pubsubSession, pattern string) {
	delete(sess.patterns, pattern)
	for i, p := range sess.porder {
		if p == pattern {
			sess.porder = append(sess.porder[:i], sess.porder[i+1:]...)
			break
		}
	}
	if sess.activeTotal() == 0 {
		reg.stopSenderLocked(sess)
	}
}

func (reg *PubSubRegistry) removeShardLocked(sess *pubsubSession, channel string) {
	delete(sess.shards, channel)
	for i, c := range sess.sorder {
		if c == channel {
			sess.sorder = append(sess.sorder[:i], sess.sorder[i+1:]...)
			break
		}
	}
	if sess.activeTotal() == 0 {
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
		reg.ensureSenderLocked(conn, sess)
		if _, dup := sess.subs[channel]; !dup {
			sess.subs[channel] = struct{}{}
			sess.order = append(sess.order, channel)
		}
		confs = append(confs, confirm("subscribe", channel, int64(sess.total())))
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
			protocol.Value{Kind: protocol.KindInteger, I: int64(sess.total())},
		))
	}
	for _, channel := range targets {
		reg.removeLocked(sess, channel)
		confs = append(confs, protocol.ArrayOf(
			protocol.BulkOf("unsubscribe"),
			protocol.BulkOf(channel),
			protocol.Value{Kind: protocol.KindInteger, I: int64(sess.total())},
		))
	}
	reg.mu.Unlock()
	return reg.emitConfs(ctx, conn, confs)
}

func (reg *PubSubRegistry) handlePsubscribe(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'psubscribe' command")
	}
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	sess := reg.sessionLocked(conn)
	confs := make([]protocol.Value, 0, len(args))
	for _, a := range args {
		pattern := string(a.Bulk)
		reg.ensureSenderLocked(conn, sess)
		if _, dup := sess.patterns[pattern]; !dup {
			sess.patterns[pattern] = struct{}{}
			sess.porder = append(sess.porder, pattern)
		}
		confs = append(confs, confirm("psubscribe", pattern, int64(sess.total())))
	}
	reg.mu.Unlock()
	return reg.emitConfs(ctx, conn, confs)
}

func (reg *PubSubRegistry) handlePunsubscribe(ctx context.Context, args []protocol.Value) protocol.Value {
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	sess := reg.sessionLocked(conn)
	var targets []string
	if len(args) == 0 {
		targets = make([]string, len(sess.porder))
		for i, p := range sess.porder {
			targets[len(sess.porder)-1-i] = p
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
			protocol.BulkOf("punsubscribe"),
			protocol.Value{Kind: protocol.KindBulkString},
			protocol.Value{Kind: protocol.KindInteger, I: int64(sess.total())},
		))
	}
	for _, pattern := range targets {
		reg.removePatternLocked(sess, pattern)
		confs = append(confs, protocol.ArrayOf(
			protocol.BulkOf("punsubscribe"),
			protocol.BulkOf(pattern),
			protocol.Value{Kind: protocol.KindInteger, I: int64(sess.total())},
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
	payload := args[1]
	return protocol.Value{Kind: protocol.KindInteger, I: reg.Publish(channel, payload)}
}

func (reg *PubSubRegistry) handleSpublish(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'spublish' command")
	}
	channel := string(args[0].Bulk)
	payload := args[1]
	return protocol.Value{Kind: protocol.KindInteger, I: reg.ShardPublish(channel, payload)}
}

func (reg *PubSubRegistry) handleSsubscribe(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'ssubscribe' command")
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
		reg.ensureSenderLocked(conn, sess)
		if _, dup := sess.shards[channel]; !dup {
			sess.shards[channel] = struct{}{}
			sess.sorder = append(sess.sorder, channel)
		}
		confs = append(confs, confirm("ssubscribe", channel, int64(sess.shardTotal())))
	}
	reg.mu.Unlock()
	return reg.emitConfs(ctx, conn, confs)
}

func (reg *PubSubRegistry) handleSunsubscribe(ctx context.Context, args []protocol.Value) protocol.Value {
	conn, errReply := reg.connOf(ctx)
	if errReply != nil {
		return *errReply
	}
	reg.mu.Lock()
	sess := reg.sessionLocked(conn)
	var targets []string
	if len(args) == 0 {
		targets = make([]string, len(sess.sorder))
		for i, c := range sess.sorder {
			targets[len(sess.sorder)-1-i] = c
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
			protocol.BulkOf("sunsubscribe"),
			protocol.Value{Kind: protocol.KindBulkString},
			protocol.Value{Kind: protocol.KindInteger, I: int64(sess.shardTotal())},
		))
	}
	for _, channel := range targets {
		reg.removeShardLocked(sess, channel)
		confs = append(confs, protocol.ArrayOf(
			protocol.BulkOf("sunsubscribe"),
			protocol.BulkOf(channel),
			protocol.Value{Kind: protocol.KindInteger, I: int64(sess.shardTotal())},
		))
	}
	reg.mu.Unlock()
	return reg.emitConfs(ctx, conn, confs)
}

// pubsubHelpText 与真机 redis 7.2.6 `PUBSUB HELP` 逐字节一致（14 元素 simple-string
// 数组，651 字节，2026-10 探针）；新增 SHARD 子命令时文本已含其描述。
var pubsubHelpText = []string{
	"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CHANNELS [<pattern>]",
	"    Return the currently active channels matching a <pattern> (default: '*').",
	"NUMPAT",
	"    Return number of subscriptions to patterns.",
	"NUMSUB [<channel> ...]",
	"    Return the number of subscribers for the specified channels, excluding",
	"    pattern subscriptions(default: no channels).",
	"SHARDCHANNELS [<pattern>]",
	"    Return the currently active shard level channels matching a <pattern> (default: '*').",
	"SHARDNUMSUB [<shardchannel> ...]",
	"    Return the number of subscribers for the specified shard level channel(s)",
	"HELP",
	"    Print this help.",
}

// handlePubSub 处理 PUBSUB 容器查询（CHANNELS/NUMSUB/NUMPAT/HELP），语义与
// 真机 redis 7.2.6 逐字节对齐（探针见 task-9-report）：
//   - CHANNELS：列普通频道（不含 pattern/shard 名），glob 过滤，字典序（真机为
//     dict 序、顺序不可承诺——字典序是最稳确定性选择，ledger 已记）。
//   - NUMSUB：扁平 [ch,n,...]，只算普通订阅；NUMPAT：全局 pattern 计数。
//   - 错误顺序：容器 arity 错（裸 PUBSUB）→ 未知子命令 → 子命令参数错。
func (reg *PubSubRegistry) handlePubSub(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return errValueStr("ERR wrong number of arguments for 'pubsub' command")
	}
	if args[0].Kind != protocol.KindBulkString {
		return errValueStr("ERR unknown subcommand ''. Try PUBSUB HELP.")
	}
	sub := string(args[0].Bulk)
	upper := strings.ToUpper(sub)
	rest := args[1:]
	if !pubsubSubs[upper] {
		return errValueStr("ERR unknown subcommand '" + sub + "'. Try PUBSUB HELP.")
	}
	switch upper {
	case "CHANNELS":
		if len(rest) > 1 {
			return errValueStr("ERR unknown subcommand or wrong number of arguments for '" +
				sub + "'. Try PUBSUB HELP.")
		}
		pattern := "*"
		if len(rest) == 1 {
			pattern = string(rest[0].Bulk)
		}
		reg.mu.Lock()
		seen := map[string]struct{}{}
		for _, sess := range reg.sessions {
			for ch := range sess.subs {
				if _, dup := seen[ch]; dup {
					continue
				}
				if matchPattern(pattern, ch) {
					seen[ch] = struct{}{}
				}
			}
		}
		reg.mu.Unlock()
		chans := make([]string, 0, len(seen))
		for ch := range seen {
			chans = append(chans, ch)
		}
		sort.Strings(chans)
		out := make([]protocol.Value, 0, len(chans))
		for _, ch := range chans {
			out = append(out, protocol.BulkOf(ch))
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	case "NUMSUB", "SHARDNUMSUB":
		reg.mu.Lock()
		defer reg.mu.Unlock()
		out := make([]protocol.Value, 0, len(rest)*2)
		for _, a := range rest {
			ch := string(a.Bulk)
			n := int64(0)
			for _, sess := range reg.sessions {
				if upper == "NUMSUB" {
					if _, ok := sess.subs[ch]; ok {
						n++
					}
				} else if _, ok := sess.shards[ch]; ok {
					n++
				}
			}
			out = append(out, protocol.BulkOf(ch), protocol.Value{Kind: protocol.KindInteger, I: n})
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	case "SHARDCHANNELS":
		if len(rest) > 1 {
			return errValueStr("ERR unknown subcommand or wrong number of arguments for '" +
				sub + "'. Try PUBSUB HELP.")
		}
		pattern := "*"
		if len(rest) == 1 {
			pattern = string(rest[0].Bulk)
		}
		reg.mu.Lock()
		seen := map[string]struct{}{}
		for _, sess := range reg.sessions {
			for ch := range sess.shards {
				if _, dup := seen[ch]; dup {
					continue
				}
				if matchPattern(pattern, ch) {
					seen[ch] = struct{}{}
				}
			}
		}
		reg.mu.Unlock()
		chans := make([]string, 0, len(seen))
		for ch := range seen {
			chans = append(chans, ch)
		}
		sort.Strings(chans)
		out := make([]protocol.Value, 0, len(chans))
		for _, ch := range chans {
			out = append(out, protocol.BulkOf(ch))
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	case "NUMPAT":
		if len(rest) != 0 {
			return errValueStr("ERR wrong number of arguments for 'pubsub|numpat' command")
		}
		reg.mu.Lock()
		total := int64(0)
		for _, sess := range reg.sessions {
			total += int64(len(sess.patterns))
		}
		reg.mu.Unlock()
		return protocol.Value{Kind: protocol.KindInteger, I: total}
	case "HELP":
		if len(rest) != 0 {
			return errValueStr("ERR wrong number of arguments for 'pubsub|help' command")
		}
		out := make([]protocol.Value, 0, len(pubsubHelpText))
		for _, line := range pubsubHelpText {
			out = append(out, protocol.Value{Kind: protocol.KindSimpleString, S: line})
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	}
	return errValueStr("ERR unknown subcommand '" + sub + "'. Try PUBSUB HELP.")
}

// Publish 向订阅 channel 的会话广播 payload，返回接收者数；与 PUBLISH
// 命令同语义，供哨兵 failover 发射 +switch-master 等服务端事件用。
func (reg *PubSubRegistry) Publish(channel string, payload protocol.Value) int64 {
	reg.mu.Lock()
	type delivery struct {
		ch   chan protocol.Value
		msgs []protocol.Value
	}
	var targets []delivery
	count := int64(0)
	for _, sess := range reg.sessions {
		if !sess.sender || sess.closed {
			continue
		}
		var msgs []protocol.Value
		if _, ok := sess.subs[channel]; ok {
			msgs = append(msgs, protocol.ArrayOf(
				protocol.BulkOf("message"),
				protocol.BulkOf(channel),
				payload,
			))
		}
		for _, pat := range sess.porder {
			if matchPattern(pat, channel) {
				msgs = append(msgs, protocol.ArrayOf(
					protocol.BulkOf("pmessage"),
					protocol.BulkOf(pat),
					protocol.BulkOf(channel),
					payload,
				))
			}
		}
		if len(msgs) > 0 {
			targets = append(targets, delivery{ch: sess.ch, msgs: msgs})
			count += int64(len(msgs))
		}
	}
	reg.mu.Unlock()
	for _, d := range targets {
		for _, m := range d.msgs {
			d.ch <- m
		}
	}
	return count
}

// ShardPublish 向 shard 订阅者投递 smessage，返回接收连接数（每连接至多 1 条，
// 与 Publish 的 len(msgs) 累加不同——真机 dualcount_probe 实测）。
func (reg *PubSubRegistry) ShardPublish(channel string, payload protocol.Value) int64 {
	reg.mu.Lock()
	type delivery struct {
		ch  chan protocol.Value
		msg protocol.Value
	}
	var targets []delivery
	count := int64(0)
	for _, sess := range reg.sessions {
		if !sess.sender || sess.closed {
			continue
		}
		if _, ok := sess.shards[channel]; ok {
			targets = append(targets, delivery{ch: sess.ch, msg: protocol.ArrayOf(
				protocol.BulkOf("smessage"),
				protocol.BulkOf(channel),
				payload,
			)})
			count++
		}
	}
	reg.mu.Unlock()
	for _, d := range targets {
		d.ch <- d.msg
	}
	return count
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
			n = sess.activeTotal()
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
