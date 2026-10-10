package commands

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

const fnSpinLib = "#!lua name=spinlib\nredis.register_function('spin', function(keys, args) while true do end end)\n"
const fnOneLib = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 1 end)\n"

func fnErrText(v protocol.Value) string {
	if v.Kind == protocol.KindError {
		return v.S
	}
	return ""
}

// fnPipeConn 供后台 goroutine 发 FCALL（避开 t.* 并发调用）。
func fnPipeConn(t *testing.T) net.Conn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return b
}

func fnDispatchBg(r *network.Router, c net.Conn, args ...string) protocol.Value {
	return dispatchFn(r, c, args...)
}

// WM: 无在飞时 FUNCTION KILL 回 NOTBUSY（真机 7.2.6 探针原文）。
func Test_Function_when_KillIdle(t *testing.T) {
	r, c := openFunctionSetup(t)
	got := dispatchFn(r, c, "FUNCTION", "KILL")
	require.Equal(t, "NOTBUSY No scripts in execution right now.", fnErrText(got))
}

// WM: KILL 多参走 function|kill arity 文案。
func Test_Function_when_KillArity(t *testing.T) {
	r, c := openFunctionSetup(t)
	got := dispatchFn(r, c, "FUNCTION", "KILL", "x")
	require.Equal(t, "ERR wrong number of arguments for 'function|kill' command", fnErrText(got))
}

// fnWaitRunning 轮询 STATS 至 running_script 出现（防调度抖动）。
func fnWaitRunning(t *testing.T, r *network.Router, c net.Conn) protocol.Value {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		st := dispatchFn(r, c, "FUNCTION", "STATS")
		if len(st.Elems) >= 2 && st.Elems[1].Kind == protocol.KindArray {
			return st.Elems[1]
		}
		if time.Now().After(deadline) {
			t.Fatal("STATS 未出现 running_script")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WM: 在飞函数可被 KILL；调用方收 SCRIPT KILL 文案；STATS 先后非空/回 nil。
func Test_Function_when_KillRunning(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, "spinlib", string(dispatchFn(r, c, "FUNCTION", "LOAD", fnSpinLib).Bulk))

	type res struct{ v protocol.Value }
	ch := make(chan res, 1)
	go func() {
		ch <- res{fnDispatchBg(r, fnPipeConn(t), "FCALL", "spin", "0")}
	}()

	running := fnWaitRunning(t, r, c)
	require.Equal(t, "spin", string(running.Elems[1].Bulk))
	var cmdToks []string
	for _, e := range running.Elems[3].Elems {
		cmdToks = append(cmdToks, string(e.Bulk))
	}
	require.Equal(t, []string{"FCALL", "spin", "0"}, cmdToks)
	require.GreaterOrEqual(t, running.Elems[5].I, int64(0))

	got := dispatchFn(r, c, "FUNCTION", "KILL")
	require.Equal(t, "OK", got.S)

	select {
	case r := <-ch:
		msg := fnErrText(r.v)
		require.True(t, strings.HasPrefix(msg, "ERR Script killed by user with SCRIPT KILL... script: spin, on @user_function:"),
			"调用方文案异常: %q", msg)
	case <-time.After(10 * time.Second):
		t.Fatal("FCALL 未返回")
	}

	st := dispatchFn(r, c, "FUNCTION", "STATS")
	require.Equal(t, protocol.KindBulkString, st.Elems[1].Kind)
	require.Nil(t, st.Elems[1].Bulk)
}

// WM: 已执行写命令的在飞函数 KILL 回 UNKILLABLE。
func Test_Function_when_KillDirty(t *testing.T) {
	r, c := openFunctionSetup(t)
	require.Equal(t, "dwlib", string(dispatchFn(r, c, "FUNCTION", "LOAD",
		"#!lua name=dwlib\nredis.register_function('dw', function(keys, args) redis.call('set','dk','v') while true do end end)\n").Bulk))
	done := make(chan protocol.Value, 1)
	go func() {
		done <- fnDispatchBg(r, fnPipeConn(t), "FCALL", "dw", "0")
	}()
	fnWaitRunning(t, r, c)
	got := dispatchFn(r, c, "FUNCTION", "KILL")
	require.True(t, strings.HasPrefix(fnErrText(got), "UNKILLABLE "), "文案异常: %q", fnErrText(got))
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("FCALL 未返回（等超时兜底）")
	}
}

// WM: DUMP→FLUSH→RESTORE roundtrip，数据与可执行性一致。
func Test_Function_when_DumpRestoreRoundtrip(t *testing.T) {
	r, c := openFunctionSetup(t)
	dispatchFn(r, c, "FUNCTION", "LOAD", fnOneLib)
	dump := dispatchFn(r, c, "FUNCTION", "DUMP")
	require.Equal(t, protocol.KindBulkString, dump.Kind)
	require.NotEmpty(t, dump.Bulk)
	dispatchFn(r, c, "FUNCTION", "FLUSH")
	require.Empty(t, fnLibNames(dispatchFn(r, c, "FUNCTION", "LIST")))
	require.Equal(t, "OK", dispatchFn(r, c, "FUNCTION", "RESTORE", string(dump.Bulk)).S)
	require.Equal(t, []string{"mylib"}, fnLibNames(dispatchFn(r, c, "FUNCTION", "LIST")))
	require.Equal(t, int64(1), dispatchFn(r, c, "FCALL", "myfunc", "0").I)
}

// WM: 空库 DUMP 可 RESTORE（幂等空操作）。
func Test_Function_when_DumpEmpty(t *testing.T) {
	r, c := openFunctionSetup(t)
	dispatchFn(r, c, "FUNCTION", "FLUSH")
	dump := dispatchFn(r, c, "FUNCTION", "DUMP")
	require.Equal(t, protocol.KindBulkString, dump.Kind)
	require.Equal(t, "OK", dispatchFn(r, c, "FUNCTION", "RESTORE", string(dump.Bulk)).S)
	require.Empty(t, fnLibNames(dispatchFn(r, c, "FUNCTION", "LIST")))
}

// WM: RESTORE 参数/载荷错误逐条对齐真机探针。
func Test_Function_when_RestoreErrors(t *testing.T) {
	r, c := openFunctionSetup(t)
	dispatchFn(r, c, "FUNCTION", "LOAD", fnOneLib)
	dump := string(dispatchFn(r, c, "FUNCTION", "DUMP").Bulk)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"缺 payload", []string{"FUNCTION", "RESTORE"}, "ERR wrong number of arguments for 'function|restore' command"},
		{"多参", []string{"FUNCTION", "RESTORE", "a", "APPEND", "x"}, "ERR unknown subcommand or wrong number of arguments for 'RESTORE'. Try FUNCTION HELP."},
		{"坏 policy", []string{"FUNCTION", "RESTORE", dump, "BAD"}, "ERR Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE."},
		{"垃圾载荷", []string{"FUNCTION", "RESTORE", "garbage"}, "ERR DUMP payload version or checksum are wrong"},
		{"空载荷", []string{"FUNCTION", "RESTORE", ""}, "ERR DUMP payload version or checksum are wrong"},
		{"截断载荷", []string{"FUNCTION", "RESTORE", dump[:len(dump)/2]}, "ERR DUMP payload version or checksum are wrong"},
		{"错魔数", []string{"FUNCTION", "RESTORE", "XXXXXXXX" + dump[8:]}, "ERR DUMP payload version or checksum are wrong"},
		{"DUMP 多参", []string{"FUNCTION", "DUMP", "x"}, "ERR wrong number of arguments for 'function|dump' command"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, fnErrText(dispatchFn(r, c, tc.args...)))
		})
	}
	// 失败不污染现存库。
	require.Equal(t, []string{"mylib"}, fnLibNames(dispatchFn(r, c, "FUNCTION", "LIST")))
}

// WM: RESTORE 冲突语义（默认 APPEND 库冲突 / REPLACE 同库替换 / 异库函数冲突）。
func Test_Function_when_RestoreCollisions(t *testing.T) {
	r, c := openFunctionSetup(t)
	dispatchFn(r, c, "FUNCTION", "LOAD", fnOneLib)
	dump := string(dispatchFn(r, c, "FUNCTION", "DUMP").Bulk)

	require.Equal(t, "ERR Library mylib already exists",
		fnErrText(dispatchFn(r, c, "FUNCTION", "RESTORE", dump)))
	require.Equal(t, "ERR Library mylib already exists",
		fnErrText(dispatchFn(r, c, "FUNCTION", "RESTORE", dump, "APPEND")))
	require.Equal(t, "OK", dispatchFn(r, c, "FUNCTION", "RESTORE", dump, "REPLACE").S)
	require.Equal(t, "OK", dispatchFn(r, c, "FUNCTION", "RESTORE", dump, "FLUSH").S)
	require.Equal(t, []string{"mylib"}, fnLibNames(dispatchFn(r, c, "FUNCTION", "LIST")))

	// 异库同名函数：APPEND 与 REPLACE 均报 Function exists。
	dispatchFn(r, c, "FUNCTION", "FLUSH")
	dispatchFn(r, c, "FUNCTION", "LOAD", "#!lua name=mylib2\nredis.register_function('myfunc', function(keys, args) return 3 end)\n")
	require.Equal(t, "ERR Function myfunc already exists",
		fnErrText(dispatchFn(r, c, "FUNCTION", "RESTORE", dump, "APPEND")))
	require.Equal(t, "ERR Function myfunc already exists",
		fnErrText(dispatchFn(r, c, "FUNCTION", "RESTORE", dump, "REPLACE")))
	require.Equal(t, []string{"mylib2"}, fnLibNames(dispatchFn(r, c, "FUNCTION", "LIST")))
}

// WM: 并发 FCALL 下 STATS 展示仍为单条 spin 记录；兜底 KILL 至全部退出。
func Test_Function_when_StatsConcurrent(t *testing.T) {
	r, c := openFunctionSetup(t)
	dispatchFn(r, c, "FUNCTION", "LOAD", fnSpinLib)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fnDispatchBg(r, fnPipeConn(t), "FCALL", "spin", "0")
		}()
	}
	fnWaitRunning(t, r, c)
	st := dispatchFn(r, c, "FUNCTION", "STATS")
	require.Equal(t, protocol.KindArray, st.Elems[1].Kind)
	require.Equal(t, "spin", string(st.Elems[1].Elems[1].Bulk))
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	for {
		select {
		case <-finished:
			return
		case <-time.After(100 * time.Millisecond):
			dispatchFn(r, c, "FUNCTION", "KILL")
		}
	}
}
