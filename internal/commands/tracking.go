package commands

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// TrackTable 维护 userKey → 连接 ID 的反向跟踪表：读注册经本表登记，
// 失效广播（T5 invalidate）按 key 反查订阅连接。fwd/rev 双索引互为镜像，
// 全部操作在同一把锁下改两侧。
type TrackTable struct {
	mu    sync.Mutex
	fwd   map[string]map[int64]struct{}
	rev   map[int64]map[string]struct{}
	bcast map[int64][]string
}

func NewTrackTable() *TrackTable {
	return &TrackTable{
		fwd:   make(map[string]map[int64]struct{}),
		rev:   make(map[int64]map[string]struct{}),
		bcast: make(map[int64][]string),
	}
}

// Register 登记 connID 跟踪 keys（幂等：重复登记同一 key 不产生重复项）。
func (t *TrackTable) Register(connID int64, keys []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range keys {
		m, ok := t.fwd[k]
		if !ok {
			m = make(map[int64]struct{})
			t.fwd[k] = m
		}
		m[connID] = struct{}{}
		rm, ok := t.rev[connID]
		if !ok {
			rm = make(map[string]struct{})
			t.rev[connID] = rm
		}
		rm[k] = struct{}{}
	}
}

// RemoveConn 清除 connID 的全部表项（正反向 + BCAST；off / 断连用）。
func (t *TrackTable) RemoveConn(connID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.bcast, connID)
	for k := range t.rev[connID] {
		if m := t.fwd[k]; m != nil {
			delete(m, connID)
			if len(m) == 0 {
				delete(t.fwd, k)
			}
		}
	}
	delete(t.rev, connID)
}

// ConnsFor 返回跟踪 key 的连接 ID 升序列表（失效广播反查与测试断言）。
func (t *TrackTable) ConnsFor(key string) []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]int64, 0, len(t.fwd[key]))
	for id := range t.fwd[key] {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// KeysFor 返回 connID 跟踪的 key 升序列表（测试断言用）。
func (t *TrackTable) KeysFor(connID int64) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.rev[connID]))
	for k := range t.rev[connID] {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// RegisterBcast 登记 connID 的 BCAST 前缀订阅（重复登记整表覆盖；
// 空列表等价 RemoveBcast）。
func (t *TrackTable) RegisterBcast(connID int64, prefixes []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(prefixes) == 0 {
		delete(t.bcast, connID)
		return
	}
	t.bcast[connID] = append([]string(nil), prefixes...)
}

// RemoveBcast 清除 connID 的 BCAST 前缀订阅（重新配置为非 BCAST 用）。
func (t *TrackTable) RemoveBcast(connID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.bcast, connID)
}

// BcastFor 返回前缀命中 key 的 BCAST 订阅连接 ID 升序列表。
func (t *TrackTable) BcastFor(key string) []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []int64
	for id, prefixes := range t.bcast {
		for _, p := range prefixes {
			if strings.HasPrefix(key, p) {
				out = append(out, id)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// InstallTrackingHook 挂读注册钩子：Dispatch 在读命令成功返回后回调本
// hook（R1 约束：只维护表，不向 conn 写任何东西）。写命令与空 keys 已
// 在 Dispatch 层过滤，此处只做 per-conn 配置判定：OPTOUT 跳过、OPTIN
// 仅 CachingYes 置位时注册（消费后复位）、BCAST/PREFIX 按前缀过滤。
func InstallTrackingHook(r *network.Router, conns *ConnRegistry, tracks *TrackTable) {
	if conns == nil || tracks == nil {
		return
	}
	r.SetReadHook(func(ctx context.Context, name string, keys []string) {
		conn, ok := network.ConnFromContext(ctx)
		if !ok || conn == nil {
			return
		}
		cfg := conns.TrackingOf(conn)
		if !cfg.On || cfg.OptOut || cfg.BCast {
			return
		}
		if cfg.OptIn && !conns.ConsumeCaching(conn) {
			return
		}
		reg := keys
		if len(cfg.Prefixes) > 0 {
			reg = reg[:0:0]
			for _, k := range keys {
				for _, p := range cfg.Prefixes {
					if strings.HasPrefix(k, p) {
						reg = append(reg, k)
						break
					}
				}
			}
		}
		if len(reg) == 0 {
			return
		}
		tracks.Register(conns.IDOf(conn), reg)
	})
}

// tracking 实现 CLIENT TRACKING on|off [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
// [PREFIX p...]：解析失败按 ERR syntax error 拒绝；OPTIN+OPTOUT 互斥；
// off 清配置与该连接全部表项。
func (h *serverHandler) tracking(ctx context.Context, args []protocol.Value) protocol.Value {
	conn, ok := trackingConn(ctx, h.conns)
	if !ok {
		return connErr("client tracking")
	}
	if len(args) < 1 {
		return errValueStr("ERR syntax error")
	}
	mode, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	on := strings.EqualFold(mode, "on")
	if !on && !strings.EqualFold(mode, "off") {
		return errValueStr("ERR syntax error")
	}
	cfg := TrackingCfg{On: on}
	seen := map[string]bool{}
	for i := 1; i < len(args); i++ {
		tok, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(tok) {
		case "BCAST", "OPTIN", "OPTOUT", "NOLOOP":
			if seen[tok] {
				return errValueStr("ERR syntax error")
			}
			seen[tok] = true
			switch strings.ToUpper(tok) {
			case "BCAST":
				cfg.BCast = true
			case "OPTIN":
				cfg.OptIn = true
			case "OPTOUT":
				cfg.OptOut = true
			case "NOLOOP":
				cfg.NoLoop = true
			}
		case "PREFIX":
			if i+1 >= len(args) {
				return errValueStr("ERR syntax error")
			}
			p, ok := argString(args[i+1])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			cfg.Prefixes = append(cfg.Prefixes, p)
			i++
		default:
			return errValueStr("ERR syntax error")
		}
	}
	if cfg.OptIn && cfg.OptOut {
		return errValueStr("ERR OPTIN and OPTOUT are not compatible")
	}
	if !on {
		untrack(conn, h.conns, h.tracks)
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	}
	h.conns.SetTracking(conn, cfg)
	h.conns.SetCaching(conn, false)
	id := h.conns.IDOf(conn)
	if cfg.BCast {
		prefixes := cfg.Prefixes
		if len(prefixes) == 0 {
			prefixes = []string{""}
		}
		h.tracks.RegisterBcast(id, prefixes)
	} else {
		h.tracks.RemoveBcast(id)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

// untrack 是跟踪态清理的单一出口（off / 断连 / 推送写错三路复用）：
// 配置清零、CachingYes 复位、正反向表与 BCAST 前缀全清。
func untrack(conn net.Conn, conns *ConnRegistry, tracks *TrackTable) {
	conns.SetTracking(conn, TrackingCfg{})
	conns.SetCaching(conn, false)
	tracks.RemoveConn(conns.IDOf(conn))
}

// caching 实现 CLIENT CACHING yes|no：tracking 未开时明确报错，
// yes/no 置/清 CachingYes（供 OPTIN 下一条带 key 命令消费）。
func (h *serverHandler) caching(ctx context.Context, args []protocol.Value) protocol.Value {
	conn, ok := trackingConn(ctx, h.conns)
	if !ok {
		return connErr("client caching")
	}
	if !h.conns.TrackingOf(conn).On {
		return errValueStr("ERR CLIENT CACHING can be called only when tracking is enabled")
	}
	if len(args) != 1 {
		return errValueStr("ERR syntax error")
	}
	v, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(v) {
	case "YES":
		h.conns.SetCaching(conn, true)
	case "NO":
		h.conns.SetCaching(conn, false)
	default:
		return errValueStr("ERR syntax error")
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

// trackingConn 取 ctx 中的连接（nil conns 视为不可用）。
func trackingConn(ctx context.Context, conns *ConnRegistry) (net.Conn, bool) {
	if conns == nil {
		return nil, false
	}
	conn, ok := network.ConnFromContext(ctx)
	if !ok || conn == nil {
		return nil, false
	}
	return conn, true
}

func connErr(what string) protocol.Value {
	return errValueStr("ERR no connection for " + what)
}
