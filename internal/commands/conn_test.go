package commands

import (
	"context"
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func openConnSetup(t testing.TB) (*network.Router, *ConnRegistry) {
	t.Helper()
	r := network.NewRouter()
	st := acl.NewStore()
	authReg := RegisterAuth(r, st)
	connReg := RegisterConn(r, st, authReg)
	return r, connReg
}

// Given: 连接层命令已注册
// When: HELLO 2 / HELLO 3
// Then: 2 回数组形（含 proto=2），3 回 map 形（含 proto=3）
func Test_Conn_when_HelloVersion(t *testing.T) {
	r, _ := openConnSetup(t)
	got := dispatch(r, "HELLO", "2")
	require.Equal(t, protocol.KindArray, got.Kind)
	found := false
	for i := 0; i+1 < len(got.Elems); i += 2 {
		if string(got.Elems[i].Bulk) == "proto" && got.Elems[i+1].I == 2 {
			found = true
		}
	}
	require.True(t, found, "HELLO 2 reply must carry proto=2: %v", got)

	got = dispatch(r, "HELLO", "3")
	require.Equal(t, protocol.KindMap, got.Kind)
	found = false
	for _, p := range got.Pairs {
		if string(p.K.Bulk) == "proto" && p.V.I == 3 {
			found = true
		}
	}
	require.True(t, found, "HELLO 3 reply must carry proto=3: %v", got)
}

// Given: HELLO
// When: 版本号非法 / AUTH 密码错误
// Then: NOPROTO / WRONGPASS
func Test_Conn_when_HelloRejects(t *testing.T) {
	r, _ := openConnSetup(t)
	got := dispatch(r, "HELLO", "9")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOPROTO")

	r2 := network.NewRouter()
	st := acl.NewStore()
	require.NoError(t, st.SetUser("default", "on", ">secret", "+@all"))
	authReg := RegisterAuth(r2, st)
	RegisterConn(r2, st, authReg)
	got = dispatch(r2, "HELLO", "3", "AUTH", "default", "bad")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGPASS")
}

// Given: default 用户设密
// When: HELLO 3 AUTH default <secret>（带连接上下文）
// Then: 回 map 形；该连接身份为 default
func Test_Conn_when_HelloAuth(t *testing.T) {
	r, _ := openConnSetup(t)
	_ = r
	r2 := network.NewRouter()
	st := acl.NewStore()
	require.NoError(t, st.SetUser("default", "on", ">secret", "+@all"))
	authReg := RegisterAuth(r2, st)
	RegisterConn(r2, st, authReg)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	got := dispatchConn(r2, c1, "HELLO", "3", "AUTH", "default", "secret")
	require.Equal(t, protocol.KindMap, got.Kind)
	require.Equal(t, "default", authReg.UserOf(c1))
}

// Given: 单库引擎
// When: SELECT 0 / SELECT 1
// Then: +OK / ERR DB index is out of range
func Test_Conn_when_Select(t *testing.T) {
	r, _ := openConnSetup(t)
	require.Equal(t,
		protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "SELECT", "0"))
	got := dispatch(r, "SELECT", "1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "out of range")
}

// Given: 回显
// When: ECHO hello / ECHO 无参
// Then: 原样 bulk / 参数错
func Test_Conn_when_Echo(t *testing.T) {
	r, _ := openConnSetup(t)
	require.Equal(t, protocol.BulkOf("hello"), dispatch(r, "ECHO", "hello"))
	got := dispatch(r, "ECHO")
	require.Equal(t, protocol.KindError, got.Kind)
}

// Given: 连接层命令 + AUTH 的 router（openConnSetup 一并注册 AUTH）
// When: COMMAND COUNT / COMMAND INFO HELLO / COMMAND GETKEYS SET k v
// Then: 7 / 条目含名+arity+@connection / [k]
func Test_Conn_when_Command(t *testing.T) {
	r, _ := openConnSetup(t)
	got := dispatch(r, "COMMAND", "COUNT")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 10}, got)

	got = dispatch(r, "COMMAND", "INFO", "HELLO")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	entry := got.Elems[0]
	require.Equal(t, protocol.KindArray, entry.Kind)
	require.Equal(t, "hello", string(entry.Elems[0].Bulk))
	require.Equal(t, int64(-1), entry.Elems[1].I)
	found := false
	for _, c := range entry.Elems[6].Elems {
		if string(c.Bulk) == "@connection" {
			found = true
		}
	}
	require.True(t, found, "COMMAND INFO HELLO must list @connection: %v", entry)

	got = dispatch(r, "COMMAND", "GETKEYS", "HELLO")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Empty(t, got.Elems)
	got = dispatch(r, "COMMAND", "GETKEYS", "BOGUS", "k")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "unknown command")
}

// Given: 带连接上下文的分发
// When: QUIT
// Then: +OK 且该连接被标记 close-after-reply
func Test_Conn_when_Quit(t *testing.T) {
	r, _ := openConnSetup(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	got := dispatchConn(r, c1, "QUIT")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, got)
	require.True(t, network.CloseRequested(c1))
	require.False(t, network.CloseRequested(c1), "flag is one-shot")
}

// Given: 真 TCP 服务
// When: 发 QUIT
// Then: 先收到 +OK，随后服务端断开（读到 EOF）
func Test_ConnAccept_when_QuitCloses(t *testing.T) {
	r, _ := openConnSetup(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := network.NewServer(r)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	ac := dialAccept(t, ln.Addr().String())
	ac.do(t, "QUIT")
	require.Equal(t, "+OK\r\n", ac.line(t))
	_, err = ac.rd.ReadString('\n')
	require.Error(t, err, "server must close connection after QUIT")
}

// Given: 已 SETNAME 的连接
// When: RESET
// Then: +RESET 且连接名被清空
func Test_Conn_when_Reset(t *testing.T) {
	r, connReg := openConnSetup(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	ctx := network.ContextWithConn(context.Background(), c1)
	connReg.SetName(c1, "web-1")
	got := r.Dispatch(ctx, cmd("RESET"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "RESET"}, got)
	require.Equal(t, "", connReg.NameOf(c1))
}
