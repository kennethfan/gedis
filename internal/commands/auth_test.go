package commands

// phase1 Task3：AUTH 双形态 + 鉴权门（accept 级，真机风格）。
// Ruling T3-R1: plan 内 openMultiHelper/dial(t) 在本仓不存在，
// 改用 cluster_accept_test.go 的 accept 模式（dialAccept/do/line）。

import (
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

// plan 自检项：ConnClosed 清理后 UserOf 回空（server 注入时判空即回 default）。
func Test_AuthRegistry_ConnClosed(t *testing.T) {
	reg := NewAuthRegistry()
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	reg.Authenticate(c1, "alice")
	require.Equal(t, "alice", reg.UserOf(c1))
	reg.ConnClosed(c1)
	require.Equal(t, "", reg.UserOf(c1))
}

func openAuthServer(t testing.TB, st *acl.Store) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: false})
	require.NoError(t, store.Open())
	r := network.DefaultRouter()
	RegisterStrings(r, store)
	reg := RegisterAuth(r, st)
	r.SetAuthorizer(st)
	srv := network.NewServer(r)
	srv.UserProvider = reg
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = store.Close()
	})
	return ln.Addr().String()
}

func Test_AuthAccept_LegacyAndUser(t *testing.T) {
	st := acl.NewStore()
	require.NoError(t, st.SetUser("alice", "on", ">alicepw", "+@all"))
	addr := openAuthServer(t, st)
	ac := dialAccept(t, addr)

	// default nopass：未认证连接按 default 身份直接放行（真机 PONG）。
	ac.do(t, "PING")
	require.Equal(t, "+PONG\r\n", ac.line(t))

	// 单参 AUTH 在 default 无口令时报错（真机同形，非 +OK）。
	ac.do(t, "AUTH", "secret")
	require.Equal(t, "-ERR AUTH <password> called with out any password configured for the default user.\r\n", ac.line(t))

	// 错误口令 → WRONGPASS。
	ac.do(t, "AUTH", "alice", "wrong")
	require.Equal(t, "-WRONGPASS invalid username-password pair or user is disabled.\r\n", ac.line(t))
	ac.do(t, "AUTH", "alice", "alicepw")
	require.Equal(t, "+OK\r\n", ac.line(t))
	ac.do(t, "PING")
	require.Equal(t, "+PONG\r\n", ac.line(t))

	// default 改写口令后：单连接仍为旧身份；新连接未认证 → NOAUTH；
	// 单参 AUTH secret 命中 default → +OK。
	require.NoError(t, st.SetUser("default", "on", ">secret", "+@all"))
	ac2 := dialAccept(t, addr)
	ac2.do(t, "PING")
	require.Equal(t, "-NOAUTH Authentication required.\r\n", ac2.line(t))
	ac2.do(t, "AUTH", "secret")
	require.Equal(t, "+OK\r\n", ac2.line(t))
	ac2.do(t, "PING")
	require.Equal(t, "+PONG\r\n", ac2.line(t))
}
