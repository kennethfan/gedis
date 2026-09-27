package network

import "context"

type userKey struct{}

// ContextWithUser 把已认证用户名注入 ctx（server.handle 每循环注入，仿 ContextWithConn 模式）。
func ContextWithUser(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, userKey{}, name)
}

// UserFromContext 取出用户名；无注入（单测直调 Dispatch）返回 ok=false。
func UserFromContext(ctx context.Context) (string, bool) {
	n, ok := ctx.Value(userKey{}).(string)
	return n, ok
}
