package commands

import (
	"context"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openFunctionSetup(t testing.TB) (*network.Router, net.Conn) {
	t.Helper()
	hub := replication.NewHub(1024)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{Hub: hub})
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	r := network.NewRouter()
	RegisterStrings(r, store)
	RegisterList(r, store, nil)
	RegisterStream(r, store, nil)
	RegisterPubSub(r)
	RegisterLua(r, 5*time.Second)
	RegisterFunctions(r, store, 5*time.Second)
	RegisterTxn(r, hub)
	srv, _ := net.Pipe()
	t.Cleanup(func() { _ = srv.Close() })
	return r, srv
}

func dispatchFn(r *network.Router, conn net.Conn, args ...string) protocol.Value {
	ctx := network.ContextWithConn(context.Background(), conn)
	return r.Dispatch(ctx, cmd(args...))
}

func fnNull() protocol.Value { return protocol.Value{Kind: protocol.KindBulkString} }

func fnLibNames(v protocol.Value) []string {
	var names []string
	for _, lib := range v.Elems {
		for i := 0; i+1 < len(lib.Elems); i += 2 {
			if string(lib.Elems[i].Bulk) == "library_name" {
				names = append(names, string(lib.Elems[i+1].Bulk))
			}
		}
	}
	sort.Strings(names)
	return names
}

// WM: FUNCTION HELP 全 40 条，元素为 simple string（真机 probe5 原文）
var functionHelpTexts = []string{
	"FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"LOAD [REPLACE] <FUNCTION CODE>",
	"    Create a new library with the given library name and code.",
	"DELETE <LIBRARY NAME>",
	"    Delete the given library.",
	"LIST [LIBRARYNAME PATTERN] [WITHCODE]",
	"    Return general information on all the libraries:",
	"    * Library name",
	"    * The engine used to run the Library",
	"    * Library description",
	"    * Functions list",
	"    * Library code (if WITHCODE is given)",
	"    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
	"STATS",
	"    Return information about the current function running:",
	"    * Function name",
	"    * Command used to run the function",
	"    * Duration in MS that the function is running",
	"    If no function is running, return nil",
	"    In addition, returns a list of available engines.",
	"KILL",
	"    Kill the current running function.",
	"FLUSH [ASYNC|SYNC]",
	"    Delete all the libraries.",
	"    When called without the optional mode argument, the behavior is determined by the",
	"    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
	"    * ASYNC: Asynchronously flush the libraries.",
	"    * SYNC: Synchronously flush the libraries.",
	"DUMP",
	"    Return a serialized payload representing the current libraries, can be restored using FUNCTION RESTORE command",
	"RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
	"    Restore the libraries represented by the given payload, it is possible to give a restore policy to",
	"    control how to handle existing libraries (default APPEND):",
	"    * FLUSH: delete all existing libraries.",
	"    * APPEND: appends the restored libraries to the existing libraries. On collision, abort.",
	"    * REPLACE: appends the restored libraries to the existing libraries, On collision, replace the old",
	"      libraries with the new libraries (notice that even on this option there is a chance of failure",
	"      in case of functions name collision with another library).",
	"HELP",
	"    Print this help.",
}

func Test_Function_when_Help(t *testing.T) {
	r, c := openFunctionSetup(t)
	got := dispatchFn(r, c, "FUNCTION", "HELP")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, len(functionHelpTexts))
	for i, want := range functionHelpTexts {
		require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: want}, got.Elems[i], "help[%d]", i)
	}
}

// WM: LOAD 头行/选项解析错误（全失败，无状态残留）
func Test_Function_when_LoadHeaderErrors(t *testing.T) {
	r, c := openFunctionSetup(t)
	const nameErr = "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long"
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no header", []string{"FUNCTION", "LOAD", "redis.register_function('a', function() return 1 end)"}, "ERR Missing library metadata"},
		{"empty code", []string{"FUNCTION", "LOAD", ""}, "ERR Missing library metadata"},
		{"comment first", []string{"FUNCTION", "LOAD", "-- c\n#!lua name=cm\nredis.register_function('a', function() return 1 end)\n"}, "ERR Missing library metadata"},
		{"blank first", []string{"FUNCTION", "LOAD", "\n#!lua name=bl\nredis.register_function('a', function() return 1 end)\n"}, "ERR Missing library metadata"},
		{"replace only", []string{"FUNCTION", "LOAD", "REPLACE"}, "ERR Missing library metadata"},
		{"code then replace", []string{"FUNCTION", "LOAD", "#!lua name=ord1 redis.register_function('a', function() return 1 end)", "REPLACE"}, "ERR Unknown option given: #!lua name=ord1 redis.register_function('a', function() return 1 end)"},
		{"no name tok", []string{"FUNCTION", "LOAD", "#!lua\nredis.register_function('a', function() return 1 end)\n"}, "ERR Library name was not given"},
		{"empty name", []string{"FUNCTION", "LOAD", "#!lua name=\nlocal x = 1\n"}, nameErr},
		{"header no newline", []string{"FUNCTION", "LOAD", "#!lua name=nn"}, "ERR Invalid library metadata"},
		{"space name", []string{"FUNCTION", "LOAD", "#!lua name= \nredis.register_function('a', function() return 1 end)\n"}, nameErr},
		{"hyphen name", []string{"FUNCTION", "LOAD", "#!lua name=my-lib\nredis.register_function('a', function() return 1 end)\n"}, nameErr},
		{"dup name tok", []string{"FUNCTION", "LOAD", "#!lua name=aa name=bb\nredis.register_function('a', function() return 1 end)\n"}, "ERR Invalid metadata value, name argument was given multiple times"},
		{"engine token", []string{"FUNCTION", "LOAD", "#!lua engine=lua name=cc\nredis.register_function('a', function() return 1 end)\n"}, "ERR Invalid metadata value given: engine=lua"},
		{"extra token", []string{"FUNCTION", "LOAD", "#!lua name=x extra\nredis.register_function('a', function() return 1 end)\n"}, "ERR Invalid metadata value given: extra"},
		{"luac engine", []string{"FUNCTION", "LOAD", "#!luac name=x\nredis.register_function('a', function() return 1 end)\n"}, "ERR Engine 'luac' not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, errValueStr(tc.want), dispatchFn(r, c, tc.args...))
		})
	}
}

// WM: LOAD 成功形态——engine 大小写不敏感、双空格、库名大小写保留
func Test_Function_when_LoadSuccess(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("sp2"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua  name=sp2\nredis.register_function('f', function() return 1 end)\n"))
	require.Equal(t, protocol.BulkOf("uplib"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!LUA name=uplib\nredis.register_function('g', function() return 1 end)\n"))
	require.Equal(t, protocol.BulkOf("UPPERLIB"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=UPPERLIB\nredis.register_function('Fx', function() return 9 end)\n"))
	require.Equal(t, intVal(9), dispatchFn(r, c, "FCALL", "Fx", "0"))
	require.Equal(t, intVal(9), dispatchFn(r, c, "FCALL", "fx", "0"))
}

// WM: LOAD 执行期错误——注册器包装 `ERR Error registering functions: ERR <inner>`
func Test_Function_when_LoadRegisterErrors(t *testing.T) {
	r, c := openFunctionSetup(t)
	const wrap = "ERR Error registering functions: ERR "
	cases := []struct {
		name string
		code string
		want string
	}{
		{"missing global type", "#!lua name=e1\nreturn type(1)\n",
			wrap + "user_function:2: Script attempted to access nonexistent global variable 'type'"},
		{"missing global call", "#!lua name=e2\nredis.call('SET','k','v')\n",
			wrap + "user_function:2: Script attempted to access nonexistent global variable 'call'"},
		{"top error", "#!lua name=e3\nerror('bang')\n",
			wrap + "user_function:2: Script attempted to access nonexistent global variable 'error'"},
		{"table no callback", "#!lua name=e4\nredis.register_function{function_name='a'}\n",
			wrap + "redis.register_function must get a callback argument"},
		{"table no name", "#!lua name=e5\nredis.register_function{callback=function() return 1 end}\n",
			wrap + "redis.register_function must get a function name argument"},
		{"table empty name", "#!lua name=e6\nredis.register_function{function_name='', callback=function() return 1 end}\n",
			wrap + "Library names can only contain letters, numbers, or underscores(_) and must be at least one character long"},
		{"flags not table", "#!lua name=e7\nredis.register_function{function_name='a', callback=function() return 1 end, flags='no-writes'}\n",
			wrap + "flags argument to redis.register_function must be a table representing function flags"},
		{"positional 3 args", "#!lua name=e8\nredis.register_function('a', function() return 1 end, {'no-writes'})\n",
			wrap + "wrong number of arguments to redis.register_function"},
		{"unknown flag", "#!lua name=e9\nredis.register_function{function_name='a', callback=function() return 1 end, flags={'bogus'}}\n",
			wrap + "unknown flag given"},
		{"within lib dup", "#!lua name=e10\nredis.register_function('a', function() return 1 end)\nredis.register_function('a', function() return 2 end)\n",
			wrap + "Function already exists in the library"},
		{"compile error", "#!lua name=e11\nreturn =1\n",
			"ERR Error compiling function: user_function:2: unexpected symbol near '='"},
		{"no functions", "#!lua name=e12\n",
			"ERR No functions registered"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, errValueStr(tc.want), dispatchFn(r, c, "FUNCTION", "LOAD", tc.code))
		})
	}
}

// WM: 冲突检查优先级——编译错/0函数/engine 错先于库存在；跨库 fn 冲突裸错；REPLACE 排除自身
func Test_Function_when_LoadConflicts(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("c1"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=c1\nredis.register_function('f', function() return 1 end)\n"))
	// 跨库函数名冲突（大小写不敏感，报新注册名）
	require.Equal(t, errValueStr("ERR Function F already exists"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=c2\nredis.register_function('F', function() return 2 end)\n"))
	// 库已存在（裸错误）
	require.Equal(t, errValueStr("ERR Library 'c1' already exists"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=c1\nredis.register_function('g', function() return 3 end)\n"))
	// 编译错先于库存在
	require.Equal(t, errValueStr("ERR Error compiling function: user_function:2: unexpected symbol near '='"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=c1\nredis.register_function('x', function() return = 1 end)\n"))
	// 0 函数先于库存在
	require.Equal(t, errValueStr("ERR No functions registered"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=c1\nlocal y = 1\n"))
	// engine 错先于库存在
	require.Equal(t, errValueStr("ERR Engine 'luac' not found"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!luac name=c1\nredis.register_function('x', function() return 1 end)\n"))
	// REPLACE 重载同库：自身函数名不冲突
	require.Equal(t, protocol.BulkOf("c1"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "REPLACE", "#!lua name=c1\nredis.register_function('f', function() return 9 end)\n"))
	require.Equal(t, intVal(9), dispatchFn(r, c, "FCALL", "f", "0"))
}

// WM: 裸 FUNCTION / 子命令 arity / unknown subcommand
func Test_Function_when_SubcommandArity(t *testing.T) {
	r, c := openFunctionSetup(t)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"bare", []string{"FUNCTION"}, "ERR wrong number of arguments for 'function' command"},
		{"unknown sub", []string{"FUNCTION", "foo"}, "ERR unknown subcommand 'foo'. Try FUNCTION HELP."},
		{"load no code", []string{"FUNCTION", "LOAD"}, "ERR wrong number of arguments for 'function|load' command"},
		{"delete no args", []string{"FUNCTION", "DELETE"}, "ERR wrong number of arguments for 'function|delete' command"},
		{"delete two args", []string{"FUNCTION", "DELETE", "lib", "x"}, "ERR wrong number of arguments for 'function|delete' command"},
		{"help extra", []string{"FUNCTION", "HELP", "x"}, "ERR wrong number of arguments for 'function|help' command"},
		{"stats extra", []string{"FUNCTION", "STATS", "x"}, "ERR wrong number of arguments for 'function|stats' command"},
		{"flush sync extra", []string{"FUNCTION", "FLUSH", "SYNC", "x"}, "ERR unknown subcommand or wrong number of arguments for 'FLUSH'. Try FUNCTION HELP."},
		{"flush bad opt", []string{"FUNCTION", "FLUSH", "BAD"}, "ERR FUNCTION FLUSH only supports SYNC|ASYNC option"},
		{"fcall arity", []string{"FCALL", "f"}, "ERR wrong number of arguments for 'fcall' command"},
		{"fcall_ro arity", []string{"FCALL_RO", "f"}, "ERR wrong number of arguments for 'fcall_ro' command"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, errValueStr(tc.want), dispatchFn(r, c, tc.args...))
		})
	}
	// FLUSH 无参 / 小写 sync → OK
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatchFn(r, c, "FUNCTION", "FLUSH"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatchFn(r, c, "FUNCTION", "FLUSH", "sync"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatchFn(r, c, "FUNCTION", "FLUSH", "SYNC"))
}

// WM: FLUSH 清空；DELETE 库名大小写敏感 + 不存在报错
func Test_Function_when_FlushDelete(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("d1"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=d1\nredis.register_function('f', function() return 1 end)\n"))
	require.Equal(t, protocol.BulkOf("case1"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=case1\nredis.register_function('g', function() return 1 end)\n"))
	require.Equal(t, errValueStr("ERR Library not found"), dispatchFn(r, c, "FUNCTION", "DELETE", "CASE1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatchFn(r, c, "FUNCTION", "DELETE", "case1"))
	require.Equal(t, errValueStr("ERR Library not found"), dispatchFn(r, c, "FUNCTION", "DELETE", "nope"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatchFn(r, c, "FUNCTION", "FLUSH"))
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}, dispatchFn(r, c, "FUNCTION", "LIST"))
	require.Equal(t, errValueStr("ERR Function not found"), dispatchFn(r, c, "FCALL", "f", "0"))
}

// WM: LIST 结构——*6 键值、flags simple string、description null、dict 序 set-equal
func Test_Function_when_ListStructure(t *testing.T) {
	r, c := openFunctionSetup(t)
	code1 := "#!lua name=l1\nredis.register_function{function_name='ro', callback=function() return 1 end, flags={'no-writes'}, description='d1'}\nredis.register_function('rf', function() return 2 end)\n"
	code2 := "#!lua name=l2\nredis.register_function('g', function() return 3 end)\n"
	code3 := "#!lua name=l3\nredis.register_function('h', function() return 4 end)\n"
	require.Equal(t, protocol.BulkOf("l1"), dispatchFn(r, c, "FUNCTION", "LOAD", code1))
	require.Equal(t, protocol.BulkOf("l2"), dispatchFn(r, c, "FUNCTION", "LOAD", code2))
	require.Equal(t, protocol.BulkOf("l3"), dispatchFn(r, c, "FUNCTION", "LOAD", code3))
	got := dispatchFn(r, c, "FUNCTION", "LIST")
	require.Equal(t, protocol.KindArray, got.Kind)
	// dict 序非字母序：按库名集合断言
	require.Equal(t, []string{"l1", "l2", "l3"}, fnLibNames(got))
	// 单库结构逐元素
	var l1 protocol.Value
	for _, lib := range got.Elems {
		if string(lib.Elems[1].Bulk) == "l1" {
			l1 = lib
		}
	}
	require.Len(t, l1.Elems, 6)
	require.Equal(t, protocol.BulkOf("library_name"), l1.Elems[0])
	require.Equal(t, protocol.BulkOf("l1"), l1.Elems[1])
	require.Equal(t, protocol.BulkOf("engine"), l1.Elems[2])
	require.Equal(t, protocol.BulkOf("LUA"), l1.Elems[3])
	require.Equal(t, protocol.BulkOf("functions"), l1.Elems[4])
	fns := l1.Elems[5]
	require.Len(t, fns.Elems, 2)
	ro := fns.Elems[0]
	require.Equal(t, protocol.BulkOf("name"), ro.Elems[0])
	require.Equal(t, protocol.BulkOf("ro"), ro.Elems[1])
	require.Equal(t, protocol.BulkOf("description"), ro.Elems[2])
	require.Equal(t, protocol.BulkOf("d1"), ro.Elems[3])
	require.Equal(t, protocol.BulkOf("flags"), ro.Elems[4])
	require.Equal(t, protocol.ArrayOf(protocol.Value{Kind: protocol.KindSimpleString, S: "no-writes"}), ro.Elems[5])
	rf := fns.Elems[1]
	require.Equal(t, protocol.BulkOf("rf"), rf.Elems[1])
	require.Equal(t, fnNull(), rf.Elems[3])
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}, rf.Elems[5])
	// WITHCODE 第 7 元素 library_code（两参数顺序均合法）
	for _, args := range [][]string{
		{"FUNCTION", "LIST", "LIBRARYNAME", "l1", "WITHCODE"},
		{"FUNCTION", "LIST", "WITHCODE", "LIBRARYNAME", "l1"},
	} {
		got = dispatchFn(r, c, args...)
		require.Len(t, got.Elems, 1, args[3])
		lib := got.Elems[0]
		require.Len(t, lib.Elems, 8)
		require.Equal(t, protocol.BulkOf("library_code"), lib.Elems[6])
		require.Equal(t, protocol.BulkOf(code1), lib.Elems[7])
	}
}

// WM: LIST 选项——未知参数 / LIBRARYNAME 缺 pattern / glob 匹配且大小写不敏感
func Test_Function_when_ListOptions(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("libW"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=libW\nredis.register_function('wt', function() return 1 end)\n"))
	require.Equal(t, errValueStr("ERR Unknown argument BADOPT"), dispatchFn(r, c, "FUNCTION", "LIST", "BADOPT"))
	require.Equal(t, errValueStr("ERR library name argument was not given"), dispatchFn(r, c, "FUNCTION", "LIST", "LIBRARYNAME"))
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}, dispatchFn(r, c, "FUNCTION", "LIST", "LIBRARYNAME", "BADOPT"))
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}, dispatchFn(r, c, "FUNCTION", "LIST", "LIBRARYNAME", "zzz*"))
	for _, pat := range []string{"libW*", "LIBW*", "LibW*", "*"} {
		got := dispatchFn(r, c, "FUNCTION", "LIST", "LIBRARYNAME", pat)
		require.Equal(t, []string{"libW"}, fnLibNames(got), "pattern %q", pat)
	}
}

// WM: STATS 结构与计数
func Test_Function_when_Stats(t *testing.T) {
	r, c := openFunctionSetup(t)
	stats := func(libraries, functions int) protocol.Value {
		return protocol.ArrayOf(
			protocol.BulkOf("running_script"), fnNull(),
			protocol.BulkOf("engines"),
			protocol.ArrayOf(
				protocol.BulkOf("LUA"),
				protocol.ArrayOf(
					protocol.BulkOf("libraries_count"), intVal(int64(libraries)),
					protocol.BulkOf("functions_count"), intVal(int64(functions)),
				),
			),
		)
	}
	require.Equal(t, stats(0, 0), dispatchFn(r, c, "FUNCTION", "STATS"))
	require.Equal(t, protocol.BulkOf("s1"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=s1\nredis.register_function('a', function() return 1 end)\n"))
	require.Equal(t, protocol.BulkOf("s2"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=s2\nredis.register_function('b', function() return 1 end)\nredis.register_function('c', function() return 2 end)\n"))
	require.Equal(t, stats(2, 3), dispatchFn(r, c, "FUNCTION", "STATS"))
}

// WM: FCALL 函数查找——大小写不敏感；未找到先于 numkeys 校验
func Test_Fcall_when_LookupAndNumkeys(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, errValueStr("ERR Function not found"), dispatchFn(r, c, "FCALL", "nope", "0"))
	require.Equal(t, protocol.BulkOf("libR"),
		dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=libR\nredis.register_function('MiXeD', function() return 'got' end)\n"))
	for _, name := range []string{"MiXeD", "mixed", "MIXED"} {
		require.Equal(t, protocol.BulkOf("got"), dispatchFn(r, c, "FCALL", name, "0"), name)
	}
	// numkeys 三错（函数存在时）
	require.Equal(t, errValueStr("ERR Bad number of keys provided"), dispatchFn(r, c, "FCALL", "MiXeD", "abc"))
	require.Equal(t, errValueStr("ERR Number of keys can't be negative"), dispatchFn(r, c, "FCALL", "MiXeD", "-1"))
	require.Equal(t, errValueStr("ERR Number of keys can't be greater than number of args"), dispatchFn(r, c, "FCALL", "MiXeD", "2"))
	// 查函数先于 numkeys
	require.Equal(t, errValueStr("ERR Function not found"), dispatchFn(r, c, "FCALL", "nope", "abc"))
	require.Equal(t, errValueStr("ERR Function not found"), dispatchFn(r, c, "FCALL_RO", "nope", "abc"))
}

// WM: FCALL 返回值映射——int/bulk/false/混合表/error_reply 裸错误/pcall 错误表裸
func Test_Fcall_when_ReturnMapping(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("retfn"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=retfn\n"+
				"redis.register_function('rf2', function() return redis.error_reply('ERR inner') end)\n"+
				"redis.register_function('rf3', function() return false end)\n"+
				"redis.register_function('rf4', function() return {1,'a',false} end)\n"+
				"redis.register_function('rf5', function() return redis.pcall('NOPECMD') end)\n"))
	require.Equal(t, errValueStr("ERR inner"), dispatchFn(r, c, "FCALL", "rf2", "0"))
	require.Equal(t, fnNull(), dispatchFn(r, c, "FCALL", "rf3", "0"))
	require.Equal(t, protocol.ArrayOf(intVal(1), protocol.BulkOf("a"), fnNull()), dispatchFn(r, c, "FCALL", "rf4", "0"))
	require.Equal(t, errValueStr("ERR Unknown Redis command called from script"), dispatchFn(r, c, "FCALL", "rf5", "0"))
}

// WM: FCALL 运行时环境——redis 表字段全表；KEYS/ARGV 全局抛错；keys/argv 走形参
func Test_Fcall_when_RuntimeEnv(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("redisfn"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=redisfn\n"+
				"redis.register_function('rf', function()\n"+
				"  local t = {}\n"+
				"  for _, n in ipairs({'call','pcall','sha1hex','log','error_reply','status_reply','register_function','LOG_DEBUG','LOG_VERBOSE','LOG_NOTICE','LOG_WARNING','LOG_NONE','redis_version','setresp'}) do\n"+
				"    local ok, v = pcall(function() return redis[n] end)\n"+
				"    t[#t+1] = n .. '=' .. (ok and type(v) or 'ERR')\n"+
				"  end\n"+
				"  return table.concat(t, ',')\n"+
				"end)\n"+
				"redis.register_function('kk', function()\n"+
				"  local ok1 = pcall(function() return KEYS end)\n"+
				"  local ok2 = pcall(function() return ARGV end)\n"+
				"  return tostring(ok1) .. ',' .. tostring(ok2)\n"+
				"end)\n"+
				"redis.register_function('af', function(keys, argv)\n"+
				"  return keys[1] .. '/' .. argv[1]\n"+
				"end)\n"+
				"redis.register_function('rl', function()\n"+
				"  redis.log(redis.LOG_DEBUG, 'rt')\n"+
				"  return type(redis.LOG_DEBUG) .. ',' .. type(redis.log)\n"+
				"end)\n"))
	require.Equal(t, protocol.BulkOf(
		"call=function,pcall=function,sha1hex=function,log=function,error_reply=function,"+
			"status_reply=function,register_function=nil,LOG_DEBUG=number,LOG_VERBOSE=number,"+
			"LOG_NOTICE=number,LOG_WARNING=number,LOG_NONE=nil,redis_version=nil,setresp=function"),
		dispatchFn(r, c, "FCALL", "rf", "0"))
	require.Equal(t, protocol.BulkOf("false,false"), dispatchFn(r, c, "FCALL", "kk", "1", "k1", "a1"))
	require.Equal(t, protocol.BulkOf("k1/a1"), dispatchFn(r, c, "FCALL", "af", "1", "k1", "a1"))
	require.Equal(t, protocol.BulkOf("number,function"), dispatchFn(r, c, "FCALL", "rl", "0"))
}

// WM: FCALL 运行时错误包装——error() 带位置；写拦截无位置；无 flag 写放行
func Test_Fcall_when_RuntimeErrors(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("rt"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=rt\n"+
				"redis.register_function('boom', function() error('bang') end)\n"))
	require.Equal(t,
		errValueStr("ERR user_function:2: bang script: boom, on @user_function:2."),
		dispatchFn(r, c, "FCALL", "boom", "0"))
	require.Equal(t, protocol.BulkOf("slib"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=slib\n"+
				"redis.register_function{function_name='setit', callback=function() return redis.call('SET', 'rtk2', 'v') end, flags={'no-writes'}}\n"))
	require.Equal(t,
		errValueStr("ERR Write commands are not allowed from read-only scripts. script: setit, on @user_function:2."),
		dispatchFn(r, c, "FCALL", "setit", "0"))
	require.Equal(t, protocol.BulkOf("wt2"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=wt2\nredis.register_function('wt', function() return redis.call('SET', 'wk', 'v') end)\n"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatchFn(r, c, "FCALL", "wt", "0"))
}

// WM: no-writes flag——无 flag 写拦截；FCALL_RO 拒绝无 flag 函数；有 flag 运行时写也拦截
func Test_Fcall_when_NoWritesFlag(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("nwlib"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=nwlib\n"+
				"redis.register_function{function_name='nw', callback=function() return redis.call('SET', 'nwk', 'v') end, flags={'no-writes'}}\n"+
				"redis.register_function('plain', function() return 1 end)\n"))
	const writeBlocked = "ERR Write commands are not allowed from read-only scripts. script: nw, on @user_function:2."
	require.Equal(t, errValueStr(writeBlocked), dispatchFn(r, c, "FCALL", "nw", "0"))
	require.Equal(t, errValueStr(writeBlocked), dispatchFn(r, c, "FCALL_RO", "nw", "0"))
	require.Equal(t, errValueStr("ERR Can not execute a script with write flag using *_ro command."),
		dispatchFn(r, c, "FCALL_RO", "plain", "0"))
	require.Equal(t, intVal(1), dispatchFn(r, c, "FCALL", "plain", "0"))
}

// WM: FCALL_RO 错误顺序——查函数 → numkeys → RO flag
func Test_FcallRo_when_ErrorsOrder(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("rolib"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=rolib\n"+
				"redis.register_function{function_name='ro', callback=function() return 2 end, flags={'no-writes'}}\n"+
				"redis.register_function('rw', function() return 1 end)\n"))
	// numkeys 非法先于 RO flag 检查
	require.Equal(t, errValueStr("ERR Bad number of keys provided"), dispatchFn(r, c, "FCALL_RO", "rw", "abc"))
	require.Equal(t, errValueStr("ERR Number of keys can't be negative"), dispatchFn(r, c, "FCALL_RO", "rw", "-1"))
	require.Equal(t, errValueStr("ERR Number of keys can't be greater than number of args"), dispatchFn(r, c, "FCALL_RO", "rw", "5"))
	require.Equal(t, errValueStr("ERR Can not execute a script with write flag using *_ro command."),
		dispatchFn(r, c, "FCALL_RO", "rw", "0"))
	require.Equal(t, intVal(2), dispatchFn(r, c, "FCALL_RO", "ro", "0"))
}

// WM: 嵌套调用——FCALL/FUNCTION/EVAL 均拒绝 from script
func Test_Fcall_when_NestedScriptCommands(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, protocol.BulkOf("wlib"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=wlib\n"+
				"redis.register_function('wt', function() return redis.call('SET', 'wk', 'v') end)\n"))
	require.Equal(t, protocol.BulkOf("nlib"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=nlib\n"+
				"redis.register_function('outer', function() return redis.call('FCALL', 'wt', 0) end)\n"))
	require.Equal(t, protocol.BulkOf("nlib2"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=nlib2\n"+
				"redis.register_function('o2', function() return redis.call('FUNCTION', 'LIST') end)\n"))
	require.Equal(t, protocol.BulkOf("nlib3"),
		dispatchFn(r, c, "FUNCTION", "LOAD",
			"#!lua name=nlib3\n"+
				"redis.register_function('o3', function() return redis.call('EVAL', 'return 1', 0) end)\n"))
	for _, fn := range []string{"outer", "o2", "o3"} {
		require.Equal(t,
			errValueStr("ERR This Redis command is not allowed from script script: "+fn+", on @user_function:2."),
			dispatchFn(r, c, "FCALL", fn, "0"), fn)
	}
}
