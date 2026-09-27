package commands

import (
	"context"
	"net"
	"sync"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// AuthStore 是 AUTH 消费的用户表最小接口，由 internal/acl.Store 实现。
type AuthStore interface {
	Authenticate(user, pass string) bool
	UserExists(user string) bool
	HasPassword(user string) bool
}

// AuthRegistry 记录每连接认证身份（server.OnConnClose 钩子清理，
// 仿 TxnRegistry conn-keyed 模式）；UserProvider 供 server.handle 注入 ctx。
type AuthRegistry struct {
	mu    sync.Mutex
	users map[net.Conn]string
}

func NewAuthRegistry() *AuthRegistry {
	return &AuthRegistry{users: make(map[net.Conn]string)}
}

func (a *AuthRegistry) Authenticate(conn net.Conn, name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.users[conn] = name
}

func (a *AuthRegistry) UserOf(c net.Conn) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.users[c]
}

func (a *AuthRegistry) ConnClosed(c net.Conn) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.users, c)
}

// RegisterAuth 注册 AUTH（双形态），返回连接身份注册表。
var authMeta = []acl.Meta{
	{Name: "AUTH", Category: "connection", Keys: acl.KeySpec{First: -1}},
}

func RegisterAuth(r *network.Router, st AuthStore) *AuthRegistry {
	for _, m := range authMeta {
		acl.RegisterMeta(m)
	}
	reg := NewAuthRegistry()
	r.Register("AUTH", func(ctx context.Context, args []protocol.Value) protocol.Value {
		strs := make([]string, len(args))
		for i, a := range args {
			s, ok := argString(a)
			if !ok {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'auth' command"}
			}
			strs[i] = s
		}
		name := "default"
		pass := ""
		switch len(strs) {
		case 1:
			if !st.HasPassword("default") {
				return protocol.Value{Kind: protocol.KindError, S: "ERR AUTH <password> called with out any password configured for the default user."}
			}
			pass = strs[0]
		case 2:
			name, pass = strs[0], strs[1]
		default:
			return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'auth' command"}
		}
		if !st.Authenticate(name, pass) {
			return protocol.Value{Kind: protocol.KindError, S: "WRONGPASS invalid username-password pair or user is disabled."}
		}
		if conn, ok := network.ConnFromContext(ctx); ok && conn != nil {
			reg.Authenticate(conn, name)
		}
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	})
	return reg
}
