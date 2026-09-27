package commands

// M8 ACL 一期验收：进程内全接线（main.go 顺序复刻）+ 真机 7.2.6 fixtures
// （testdata/acl/，端口 6390 录制）：AUTH 双形态、WRONGPASS/NOAUTH/NOPERM
// 逐字节，ACL LIST 稳定 token 同形（sanitize-payload/resetchannels 为版本
// 噪音，仅断言 user/on/#/规则 token，见 Ruling T5-R4）。

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openACLAccept(t testing.TB) string {
	t.Helper()
	st := acl.NewStore()
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: false})
	require.NoError(t, store.Open())
	r := network.DefaultRouter()
	RegisterStrings(r, store)
	reg := RegisterAuth(r, st)
	RegisterACL(r, st, reg, nil)
	r.SetAuthorizer(st)
	srv := network.NewServer(r)
	srv.UserProvider = reg
	srv.OnConnClose(func(c net.Conn) { reg.ConnClosed(c) })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = store.Close()
	})
	return ln.Addr().String()
}

func aclFixture(t testing.TB, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "acl", name))
	require.NoError(t, err)
	return string(raw)
}

func Test_ACLAccept_Fixtures(t *testing.T) {
	addr := openACLAccept(t)
	ac := dialAccept(t, addr)

	// 建与录制一致的用户：alice(+@all)/bob(+@all -GET)，default 保持 nopass。
	ac.do(t, "ACL", "SETUSER", "alice", "on", ">secret", "+@all")
	require.Equal(t, "+OK\r\n", ac.line(t))
	ac.do(t, "ACL", "SETUSER", "bob", "on", ">pw", "+@all", "-GET")
	require.Equal(t, "+OK\r\n", ac.line(t))

	// WRONGPASS / +OK / 单参无口令 default 报错，逐字节对 fixture。
	ac.do(t, "AUTH", "alice", "wrong")
	require.Equal(t, aclFixture(t, "auth_wrongpass.bin"), ac.line(t))
	ac.do(t, "AUTH", "alice", "secret")
	require.Equal(t, aclFixture(t, "auth_ok.bin"), ac.line(t))
	ac.do(t, "AUTH", "secret")
	require.Equal(t, aclFixture(t, "auth_nopass_default_err.bin"), ac.line(t))

	// default nopass：未认证 PING 按 default 放行（真机 +PONG）。
	ac2 := dialAccept(t, addr)
	ac2.do(t, "PING")
	require.Equal(t, "+PONG\r\n", ac2.line(t))

	// bob 越权 GET → NOPERM 逐字节。
	ac.do(t, "AUTH", "bob", "pw")
	require.Equal(t, "+OK\r\n", ac.line(t))
	ac.do(t, "GET", "x")
	require.Equal(t, aclFixture(t, "noperm_bob_get.bin"), ac.line(t))

	// default 设口令后：新连接 PING → NOAUTH 逐字节；单参 AUTH 恢复 +OK。
	ac.do(t, "AUTH", "alice", "secret")
	require.Equal(t, "+OK\r\n", ac.line(t))
	ac.do(t, "ACL", "SETUSER", "default", "on", ">dpass", "+@all")
	require.Equal(t, "+OK\r\n", ac.line(t))
	ac3 := dialAccept(t, addr)
	ac3.do(t, "PING")
	require.Equal(t, aclFixture(t, "noauth.bin"), ac3.line(t))
	ac3.do(t, "AUTH", "dpass")
	require.Equal(t, aclFixture(t, "auth_ok.bin"), ac3.line(t))

	// LIST：fixture 行稳定 token（user 名/on/#/规则）逐项 Contains。
	ac.do(t, "ACL", "LIST")
	list := ac.value(t)
	got := map[string]string{}
	for _, e := range list.Elems {
		s := string(e.Bulk)
		got[strings.SplitN(s, " ", 3)[1]] = s
	}
	for _, line := range strings.Split(strings.TrimRight(aclFixture(t, "list.redis"), "\n"), "\n") {
		f := strings.Fields(line)
		name := f[1]
		ours, ok := got[name]
		require.True(t, ok, "missing user %s", name)
		require.Contains(t, ours, "user "+name+" on")
		for _, tok := range f {
			if strings.HasPrefix(tok, "#") || strings.HasPrefix(tok, "+") || strings.HasPrefix(tok, "-") {
				require.Contains(t, strings.ToLower(ours), strings.ToLower(tok), "user %s", name)
			}
		}
	}
}
