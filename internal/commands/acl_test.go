package commands

// phase1 Task4：ACL 用户管理命令（accept 级）。

import (
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func Test_ACLAccept_UserLifecycle(t *testing.T) {
	st := acl.NewStore()
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: false})
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	r := network.DefaultRouter()
	RegisterStrings(r, store)
	reg := RegisterAuth(r, st)
	RegisterACL(r, st, reg, nil)
	r.SetAuthorizer(st)
	srv := network.NewServer(r)
	srv.UserProvider = reg
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	ac := dialAccept(t, ln.Addr().String())

	ac.do(t, "ACL", "WHOAMI")
	who := ac.value(t)
	require.Equal(t, protocol.KindBulkString, who.Kind)
	require.Equal(t, "default", string(who.Bulk))

	ac.do(t, "ACL", "SETUSER", "alice", "on", ">alicepw", "+@all")
	require.Equal(t, "+OK\r\n", ac.line(t))

	// alice 建好后进入受限模式：AUTH 为 alice，后续按 alice 身份放行。
	ac.do(t, "AUTH", "alice", "alicepw")
	require.Equal(t, "+OK\r\n", ac.line(t))

	ac.do(t, "ACL", "USERS")
	users := ac.value(t)
	require.Len(t, users.Elems, 2)
	require.Equal(t, "alice", string(users.Elems[0].Bulk))
	require.Equal(t, "default", string(users.Elems[1].Bulk))

	ac.do(t, "ACL", "LIST")
	list := ac.value(t)
	require.Len(t, list.Elems, 2)
	require.Contains(t, string(list.Elems[0].Bulk), "user alice on #")

	ac.do(t, "ACL", "GETUSER", "alice")
	gu := ac.value(t)
	require.Equal(t, protocol.KindArray, gu.Kind)

	ac.do(t, "ACL", "DELUSER", "default")
	require.Contains(t, ac.line(t), "-ERR")

	ac.do(t, "ACL", "SETUSER", "bob", "on", ">bobpw", "+@all")
	require.Equal(t, "+OK\r\n", ac.line(t))
	ac.do(t, "ACL", "DELUSER", "bob", "nobody")
	require.Equal(t, ":1\r\n", ac.line(t))

	// 删当前认证用户（alice）→ 拒绝。
	ac.do(t, "ACL", "DELUSER", "alice")
	require.Contains(t, ac.line(t), "-ERR")

	ac.do(t, "ACL", "SAVE")
	require.Contains(t, ac.line(t), "-ERR")
}
