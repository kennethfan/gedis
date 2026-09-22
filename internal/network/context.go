package network

import (
	"context"
	"net"
)

type connKey struct{}

type statsKey struct{}

// ContextWithConn 把活跃连接注入 ctx，供 PSYNC 等需劫持连接的 handler 用。
func ContextWithConn(ctx context.Context, conn net.Conn) context.Context {
	return context.WithValue(ctx, connKey{}, conn)
}

// ConnFromContext 取出连接；dispatch 单测等无连接场景返回 ok=false。
func ConnFromContext(ctx context.Context) (net.Conn, bool) {
	conn, ok := ctx.Value(connKey{}).(net.Conn)
	return conn, ok
}

// ContextWithStats 把统计容器注入 ctx（server.handle 注入，
// lookupRaw 被动过期计数用）；无 stats 返回原 ctx。
func ContextWithStats(ctx context.Context, s *Stats) context.Context {
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, statsKey{}, s)
}

// StatsFromContext 取出统计容器；无则返回 nil（调用方判空）。
func StatsFromContext(ctx context.Context) *Stats {
	s, _ := ctx.Value(statsKey{}).(*Stats)
	return s
}
