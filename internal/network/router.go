package network

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/protocol"
)

// Handler 处理一条已解析的命令，args 为命令名之后的参数。
type Handler func(ctx context.Context, args []protocol.Value) protocol.Value

// InterceptFunc 在正常分发前运行；handled=true 时 reply 为最终回复。
// 典型用途：MULTI 会话排队拦截（见 commands.RegisterTxn）。
type InterceptFunc func(ctx context.Context, cmd protocol.Value) (reply protocol.Value, handled bool)

// AuthorizerStore 是鉴权门消费的用户表最小接口，由 internal/acl.Store 实现。
type AuthorizerStore interface {
	CanRun(user, cmd string) bool
	Check(user, cmd string, keys, channels []string) error
	LogDenied(client, cmd, reason string)
	UserExists(user string) bool
	HasRestrictedUsers() bool
	DefaultRequiresAuth() bool
}

// Router 按命令名（大小写不敏感）分发，并发安全。
// AttachStats 后对每次分发计数并记录慢查询；不挂载则零开销。
type Router struct {
	mu          sync.RWMutex
	handlers    map[string]Handler
	stats       *Stats
	readonly    bool
	writeCmds   map[string]bool
	intercept   InterceptFunc
	auth        AuthorizerStore
	monitorHook MonitorHookFunc
	readHook    ReadHookFunc
}

// MonitorHookFunc 是 MONITOR 观测钩子：auth 通过、handler 命中后、执行前调用；
// 实现必须非阻塞（内部用有界缓冲）。
type MonitorHookFunc func(ctx context.Context, name string, args []protocol.Value)

// ReadHookFunc 是读注册钩子：auth 通过、读命令 handler 成功返回后、回包前
// 调用（keys 已非空、写命令已在 Dispatch 层过滤）；实现必须非阻塞，
// 且不得向 conn 写任何数据（跟踪表只做内存维护）。
type ReadHookFunc func(ctx context.Context, name string, keys []string)

func NewRouter() *Router {
	return &Router{handlers: make(map[string]Handler)}
}

func DefaultRouter() *Router {
	r := NewRouter()
	r.Register("PING", handlePing)
	return r
}

func (r *Router) Register(name string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[strings.ToUpper(name)] = h
}

// Handler 取已注册 handler（大小写不敏感），供命令包裹复用（如订阅态 PING）。
func (r *Router) Handler(name string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[strings.ToUpper(name)]
	return h, ok
}

// AttachStats 挂载统计容器，可挂载多次（以后者为准）。
func (r *Router) AttachStats(s *Stats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats = s
}

// SetReadOnly 开关只读副本模式；SetWriteCommands 声明写命令集合。
func (r *Router) SetReadOnly(readonly bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readonly = readonly
}

func (r *Router) SetWriteCommands(cmds map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeCmds = cmds
}

// SetAuthorizer 挂载鉴权表；nil 表示关闭鉴权门。
func (r *Router) SetAuthorizer(st AuthorizerStore) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.auth = st
}

// Authorizer 返回挂载的鉴权表（Lua 脚本内检查用）；nil 表关闭。
func (r *Router) Authorizer() AuthorizerStore {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.auth
}

// isAuthExempt 是未认证连接仍可执行的命令（AUTH/HELLO/QUIT）。
func isAuthExempt(name string) bool {
	return name == "AUTH" || name == "HELLO" || name == "QUIT"
}

// SetIntercept 设置分发前钩子（启动时调一次）；传 nil 卸载。
func (r *Router) SetIntercept(fn InterceptFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.intercept = fn
}

func (r *Router) SetMonitorHook(fn MonitorHookFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.monitorHook = fn
}

// SetReadHook 挂读注册钩子（启动时调一次）；传 nil 卸载。
func (r *Router) SetReadHook(fn ReadHookFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readHook = fn
}

// ChainIntercept 追挂分发前钩子：与既有钩子按注册顺序依次尝试，首个
// handled=true 者胜出。供多 registry 共存时用（如 Txn + PubSub 的订阅态拦截）。
func (r *Router) ChainIntercept(fn InterceptFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.intercept == nil {
		r.intercept = fn
		return
	}
	prev := r.intercept
	r.intercept = func(ctx context.Context, cmd protocol.Value) (protocol.Value, bool) {
		if reply, handled := prev(ctx, cmd); handled {
			return reply, true
		}
		return fn(ctx, cmd)
	}
}

// Has 查命令名是否已注册（大小写不敏感）。
func (r *Router) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.handlers[name]
	return ok
}

// Commands 列出全部已注册命令名（大写）。
func (r *Router) Commands() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		out = append(out, name)
	}
	return out
}

// Dispatch 解析命令名并调用对应 handler；未知命令或畸形输入返回 Error。
func (r *Router) Dispatch(ctx context.Context, cmd protocol.Value) protocol.Value {
	if cmd.Kind != protocol.KindArray || len(cmd.Elems) == 0 {
		return errValue("ERR wrong number of arguments for '' command")
	}
	name, ok := cmdName(cmd.Elems[0])
	if !ok {
		return errValue("ERR invalid command name")
	}
	r.mu.RLock()
	h, ok := r.handlers[name]
	stats := r.stats
	readonly := r.readonly && r.writeCmds[name]
	writable := r.writeCmds[name]
	intercept := r.intercept
	auth := r.auth
	monitorHook := r.monitorHook
	readHook := r.readHook
	r.mu.RUnlock()
	if intercept != nil {
		if reply, handled := intercept(ctx, cmd); handled {
			if stats != nil {
				stats.incCommands()
			}
			return reply
		}
	}
	if auth != nil && auth.HasRestrictedUsers() && !isAuthExempt(name) {
		user, ok := UserFromContext(ctx)
		authed := ok && user != ""
		if !authed {
			user = "default"
		}
		if !authed && auth.DefaultRequiresAuth() {
			if stats != nil {
				stats.incCommands()
			}
			return errValue("NOAUTH Authentication required.")
		}
		if err := auth.Check(user, name, extractKeys(name, cmd.Elems[1:]), extractChannels(name, cmd.Elems[1:])); err != nil {
			if stats != nil {
				stats.incCommands()
			}
			client := "test"
			if conn, ok := ConnFromContext(ctx); ok && conn != nil {
				client = conn.RemoteAddr().String()
			}
			auth.LogDenied(client, name, err.Error())
			return errValue(err.Error())
		}
	}
	if !ok {
		if stats != nil {
			stats.incCommands()
		}
		return UnknownCommandReply(cmd)
	}
	if readonly {
		if stats != nil {
			stats.incCommands()
		}
		return errValue("READONLY You can't write against a read only replica.")
	}
	var start time.Time
	if stats != nil {
		stats.incCommands()
		start = time.Now()
	}
	if monitorHook != nil {
		monitorHook(ctx, name, cmd.Elems[1:])
	}
	reply := h(ctx, cmd.Elems[1:])
	if stats != nil {
		micros := time.Since(start).Microseconds()
		if micros >= stats.SlowThresholdMicros && name != "SLOWLOG" {
			args := make([]string, 0, len(cmd.Elems)-1)
			for _, a := range cmd.Elems[1:] {
				if s, ok := bulkString(a); ok {
					args = append(args, s)
				}
			}
			stats.AddSlow(strings.ToLower(name), args, micros)
		}
	}
	if readHook != nil && !writable && reply.Kind != protocol.KindError {
		if keys := extractKeys(name, cmd.Elems[1:]); len(keys) > 0 {
			readHook(ctx, name, keys)
		}
	}
	return reply
}

func cmdName(v protocol.Value) (string, bool) {
	if v.Kind != protocol.KindBulkString {
		return "", false
	}
	return strings.ToUpper(string(v.Bulk)), true
}

func errValue(msg string) protocol.Value {
	return protocol.Value{Kind: protocol.KindError, S: msg}
}

// UnknownCommandReply 拼与 Redis 7.2 逐字节一致的未知命令错误（含 with args beginning with 后缀）。
func UnknownCommandReply(cmd protocol.Value) protocol.Value {
	var sb strings.Builder
	fmt.Fprintf(&sb, "ERR unknown command '%s', with args beginning with: ", string(cmd.Elems[0].Bulk))
	for _, a := range cmd.Elems[1:] {
		if a.Kind == protocol.KindBulkString {
			fmt.Fprintf(&sb, "'%s' ", string(a.Bulk))
		}
	}
	return errValue(sb.String())
}

func handlePing(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) > 0 {
		if s, ok := bulkString(args[0]); ok {
			return protocol.BulkOf(s)
		}
		return args[0]
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "PONG"}
}

func bulkString(v protocol.Value) (string, bool) {
	if v.Kind != protocol.KindBulkString {
		return "", false
	}
	return string(v.Bulk), true
}

// bulkArgs 取 bulk 参数原文（非 bulk 跳过，与 slowlog 惯例一致）。
func bulkArgs(elems []protocol.Value) []string {
	out := make([]string, 0, len(elems))
	for _, e := range elems {
		if s, ok := bulkString(e); ok {
			out = append(out, s)
		}
	}
	return out
}

// extractKeys 按 key-spec 从参数提 key；未知命令返回空（key 阶段放行）。
func extractKeys(name string, elems []protocol.Value) []string {
	return acl.ExtractKeys(name, bulkArgs(elems))
}

// extractChannels 提订阅/发布命令的 channel 参数。
func extractChannels(name string, elems []protocol.Value) []string {
	args := bulkArgs(elems)
	switch name {
	case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE":
		return args
	case "PUBLISH":
		if len(args) > 0 {
			return args[:1]
		}
	}
	return nil
}
