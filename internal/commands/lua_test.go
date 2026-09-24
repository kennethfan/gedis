package commands

import (
	"context"
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openLuaSetup(t testing.TB) (*network.Router, net.Conn) {
	t.Helper()
	hub := replication.NewHub(1024)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{Hub: hub})
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	r := network.NewRouter()
	RegisterStrings(r, store)
	RegisterList(r, store, nil)
	RegisterLua(r)
	RegisterTxn(r, hub)
	srv, _ := net.Pipe()
	t.Cleanup(func() { _ = srv.Close() })
	return r, srv
}

func dispatchLua(r *network.Router, conn net.Conn, args ...string) protocol.Value {
	ctx := network.ContextWithConn(context.Background(), conn)
	return r.Dispatch(ctx, cmd(args...))
}

func intVal(i int64) protocol.Value { return protocol.Value{Kind: protocol.KindInteger, I: i} }

// WM: 基本返回值映射（int/string/true→1/false→nil/小数截断/嵌套拍平）
func Test_Lua_when_ReturnMapping(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return 1", "0"))
	require.Equal(t, protocol.BulkOf("hello"), dispatchLua(r, c, "EVAL", "return 'hello'", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return true", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString},
		dispatchLua(r, c, "EVAL", "return false", "0"))
	require.Equal(t, intVal(3), dispatchLua(r, c, "EVAL", "return 3.5", "0"))
	require.Equal(t,
		protocol.ArrayOf(protocol.BulkOf("a"), protocol.BulkOf("b"), protocol.BulkOf("c")),
		dispatchLua(r, c, "EVAL", "return {'a',{'b','c'}}", "0"))
	require.Equal(t,
		protocol.ArrayOf(intVal(1), intVal(2), intVal(3)),
		dispatchLua(r, c, "EVAL", "return {1,2,3}", "0"))
}

// WM: KEYS/ARGV 按序传入
func Test_Lua_when_KeysArgv(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t,
		protocol.ArrayOf(protocol.BulkOf("a"), protocol.BulkOf("b"), protocol.BulkOf("c")),
		dispatchLua(r, c, "EVAL", "return {KEYS[1],KEYS[2],ARGV[1]}", "2", "a", "b", "c"))
	require.Equal(t, intVal(0), dispatchLua(r, c, "EVAL", "return #KEYS", "0"))
}

// WM: redis.call 读写往返；GET 缺失回 false
func Test_Lua_when_CallRoundtrip(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchLua(r, c, "EVAL", "return redis.call('SET','k','v')", "0"))
	require.Equal(t, protocol.BulkOf("v"),
		dispatchLua(r, c, "EVAL", "return redis.call('GET','k')", "0"))
	require.Equal(t, intVal(1),
		dispatchLua(r, c, "EVAL", "return redis.call('GET','missing') == false", "0"))
	require.Equal(t, intVal(2),
		dispatchLua(r, c, "EVAL", "redis.call('SET','w','1'); return redis.call('INCR','w')", "0"))
}

// WM: call 错误向外抛（带 script 后缀），pcall 装 {err} 表原文返回
func Test_Lua_when_CallVsPcallError(t *testing.T) {
	r, c := openLuaSetup(t)
	dispatchLua(r, c, "EVAL", "return redis.call('SET','k','v')", "0")
	script := "return redis.call('INCR','k')"
	sha := sha1Hex(script)
	require.Equal(t,
		protocol.Value{Kind: protocol.KindError,
			S: "ERR value is not an integer or out of range script: " + sha + ", on @user_script:1."},
		dispatchLua(r, c, "EVAL", script, "0"))
	require.Equal(t, protocol.BulkOf("ERR value is not an integer or out of range"),
		dispatchLua(r, c, "EVAL", "local r=redis.pcall('INCR','k'); return r.err", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError, S: "ERR value is not an integer or out of range"},
		dispatchLua(r, c, "EVAL", "local r=redis.pcall('INCR','k'); return r", "0"))
}

// WM: WRONGTYPE 不补 ERR 前缀；未知命令与 arity 由桥接层报错
func Test_Lua_when_BridgeErrors(t *testing.T) {
	r, c := openLuaSetup(t)
	dispatchLua(r, c, "EVAL", "return redis.call('SET','sk','v')", "0")
	got := dispatchLua(r, c, "EVAL", "return redis.call('LPUSH','sk','v')", "0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGTYPE Operation against a key holding the wrong kind of value script: ")
	require.NotContains(t, got.S, "ERR WRONGTYPE")
	require.Equal(t, protocol.KindError,
		dispatchLua(r, c, "EVAL", "return redis.call('NOSUCHCMD')", "0").Kind)
	got = dispatchLua(r, c, "EVAL", "return redis.call('NOSUCHCMD')", "0")
	require.Contains(t, got.S, "ERR Unknown Redis command called from script script: ")
	got = dispatchLua(r, c, "EVAL", "return redis.call('GET','a','b')", "0")
	require.Contains(t, got.S, "ERR Wrong number of args calling Redis command from script script: ")
}

// WM: {err}/{ok} 与 status_reply/error_reply 映射
func Test_Lua_when_ErrOkTables(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindError, S: "my error"},
		dispatchLua(r, c, "EVAL", "return {err='my error'}", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "fine"},
		dispatchLua(r, c, "EVAL", "return {ok='fine'}", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "fine"},
		dispatchLua(r, c, "EVAL", "return redis.status_reply('fine')", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError, S: "ERR bad"},
		dispatchLua(r, c, "EVAL", "return redis.error_reply('bad')", "0"))
	require.Equal(t, protocol.BulkOf("a9993e364706816aba3e25717850c26c9cd0d89d"),
		dispatchLua(r, c, "EVAL", "return redis.sha1hex('abc')", "0"))
}

// WM: error() 与编译错误的外层格式
func Test_Lua_when_ScriptErrors(t *testing.T) {
	r, c := openLuaSetup(t)
	script := "error('boom')"
	sha := sha1Hex(script)
	require.Equal(t,
		protocol.Value{Kind: protocol.KindError,
			S: "ERR user_script:1: boom script: " + sha + ", on @user_script:1."},
		dispatchLua(r, c, "EVAL", script, "0"))
	got := dispatchLua(r, c, "EVAL", "return {{{", "0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "ERR Error compiling script (new function): user_script:1: ")
}

// WM: numkeys 非法三件套 + argc 不足
func Test_Lua_when_BadNumkeys(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindError,
		S: "ERR value is not an integer or out of range"},
		dispatchLua(r, c, "EVAL", "return 1", "foo"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError, S: "ERR Number of keys can't be negative"},
		dispatchLua(r, c, "EVAL", "return 1", "-1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError,
		S: "ERR Number of keys can't be greater than number of args"},
		dispatchLua(r, c, "EVAL", "return 1", "5", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError,
		S: "ERR wrong number of arguments for 'eval' command"},
		dispatchLua(r, c, "EVAL", "return 1"))
}

// WM: SCRIPT LOAD→EVALSHA→EXISTS→FLUSH→NOSCRIPT；EVAL 自动缓存
func Test_Lua_when_ScriptCache(t *testing.T) {
	r, c := openLuaSetup(t)
	sha := sha1Hex("return 1")
	require.Equal(t, protocol.BulkOf(sha), dispatchLua(r, c, "SCRIPT", "LOAD", "return 1"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVALSHA", sha, "0"))
	require.Equal(t, protocol.ArrayOf(intVal(1), intVal(0)),
		dispatchLua(r, c, "SCRIPT", "EXISTS", sha, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchLua(r, c, "SCRIPT", "FLUSH"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError,
		S: "NOSCRIPT No matching script. Please use EVAL."},
		dispatchLua(r, c, "EVALSHA", sha, "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return 2-1", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVALSHA", sha1Hex("return 2-1"), "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindError,
		S: "ERR unknown subcommand 'NOSUCH'. Try SCRIPT HELP."},
		dispatchLua(r, c, "SCRIPT", "NOSUCH"))
}

// WM: MULTI 内 EVAL 整体排队，EXEC 回放执行脚本内写
func Test_Lua_when_EvalInMulti(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchLua(r, c, "MULTI"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "QUEUED"},
		dispatchLua(r, c, "EVAL", "return redis.call('SET','mk','1')", "0"))
	got := dispatchLua(r, c, "EXEC")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, got.Elems[0])
	require.Equal(t, protocol.BulkOf("1"), dispatchLua(r, c, "EVAL", "return redis.call('GET','mk')", "0"))
}
