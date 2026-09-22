package network

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
)

// Handler 处理一条已解析的命令，args 为命令名之后的参数。
type Handler func(ctx context.Context, args []protocol.Value) protocol.Value

// Router 按命令名（大小写不敏感）分发，并发安全。
// AttachStats 后对每次分发计数并记录慢查询；不挂载则零开销。
type Router struct {
	mu        sync.RWMutex
	handlers  map[string]Handler
	stats     *Stats
	readonly  bool
	writeCmds map[string]bool
}

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
	r.mu.RUnlock()
	if !ok {
		if stats != nil {
			stats.incCommands()
		}
		return errValue(fmt.Sprintf("ERR unknown command '%s'", string(cmd.Elems[0].Bulk)))
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
