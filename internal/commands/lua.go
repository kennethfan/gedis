package commands

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// LuaRegistry 持脚本缓存（sha1 小写 hex → 脚本正文），EVAL 自动缓存。
// 线级语义与 Redis 7.2 对齐（2026-09 探针，redis 7.2.6）：
//   - Lua→RESP：number 向零截断取整；string→bulk；true→:1；false/nil→null；
//     数组按序转换且嵌套表拍平（真机 {{1,2},{3}} 回 1 2 3）；单键 {err=}→error（原文）、
//     {ok=}→status。
//   - 脚本错误外层格式：`<msg> script: <sha>, on @user_script:1.`，msg 为 Lua 运行时
//     错误时前补 `ERR `（WRONGTYPE 与自带 `ERR ` 前缀的不补）；编译错误前补
//     `ERR Error compiling script (new function): `。
//   - redis.call/pcall 直调 Router.Handler（绕 intercept，MULTI 内 EVAL 整体排队、
//     脚本内调用不被误排队）；未知命令/非字符串命令名→
//     `ERR Unknown Redis command called from script`；arity 不符→
//     `ERR Wrong number of args calling Redis command from script`（均参与外层后缀）。
//   - EVALSHA 缺失→`NOSCRIPT No matching script. Please use EVAL.`；EVAL 自动缓存；
//     SCRIPT LOAD/EXISTS/FLUSH 语义对齐；未知子命令→
//     `ERR unknown subcommand '<X>'. Try SCRIPT HELP.`。
//   - 每 EVAL 一副新 LState + 5s context 超时（对齐 lua-time-limit 默认，防死循环 hang 住连接）。
//   - SCRIPT KILL：跨连接中止在飞脚本；无运行→NOTBUSY，已执行写命令→
//     UNKILLABLE（dirty 规则经 7.2.6 探针：分发执行的写命令即脏，
//     回复错误也算；未知命令/arity 等分发前拒绝不算脏）。
// v1 非目标：可调 lua-time-limit、阻塞命令限制、cjson/cmsgpack、
// 从库脚本内写拦截（直调 handler 绕过 readonly 门，注释备案）。
type LuaRegistry struct {
	mu      sync.Mutex
	scripts map[string]string
	running map[*luaRun]struct{}
}

// luaRun 是一次在飞脚本执行的 KILL 句柄。字段仅在 reg.mu 下读写：
// EVAL 侧（PCall 阻塞中）由 registerRedisLib 闭包置 dirty，KILL 侧读 dirty
// 后调 cancel 中断 gopher-lua 主循环（vm.go 每轮 select ctx.Done）。
type luaRun struct {
	cancel context.CancelFunc
	dirty  bool
	killed bool
}

// track 登记在飞脚本；untrack 注销（run defer）。
func (r *LuaRegistry) track(rr *luaRun) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running == nil {
		r.running = make(map[*luaRun]struct{})
	}
	r.running[rr] = struct{}{}
}

func (r *LuaRegistry) untrack(rr *luaRun) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.running, rr)
}

// kill 中止全部在飞的干净脚本。任一已脏则整体 UNKILLABLE（真机单脚本
// 语义的保守推广：gedis 并发执行可有多在飞，杀一半留一半更迷惑）。
// 返回 (killed, unkillable)：无在飞时均为 false（NOTBUSY）。
func (r *LuaRegistry) kill() (killed, unkillable bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.running) == 0 {
		return false, false
	}
	for rr := range r.running {
		if rr.dirty {
			return false, true
		}
	}
	for rr := range r.running {
		rr.killed = true
		rr.cancel()
	}
	return true, false
}

// markDirty 标记某次执行已分发过写命令（分发前拒绝的不调此函数）。
func (r *LuaRegistry) markDirty(rr *luaRun) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rr.dirty = true
}

// wasKilled 供 run 在 PCall 出错后判定是否走 killed 文案（而非 ctx 原文）。
func (r *LuaRegistry) wasKilled(rr *luaRun) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return rr.killed
}

// RegisterLua 注册 EVAL/EVALSHA/SCRIPT；返回 registry（纯缓存，无连接状态，无需 ConnClosed）。
// timeout 为单脚本执行上限（0 表示不限，kill 仍可中断）；默认 5s 由调用方按配置传入。
func RegisterLua(r *network.Router, timeout time.Duration) *LuaRegistry {
	reg := &LuaRegistry{scripts: make(map[string]string)}
	exec := &luaExec{router: r, reg: reg, timeout: timeout}
	r.Register("EVAL", exec.handleEval)
	r.Register("EVALSHA", exec.handleEvalSHA)
	r.Register("SCRIPT", exec.handleScript)
	return reg
}

type luaExec struct {
	router  *network.Router
	reg     *LuaRegistry
	timeout time.Duration
}

func sha1Hex(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (e *luaExec) cached(body string) string {
	sha := sha1Hex(body)
	e.reg.mu.Lock()
	e.reg.scripts[sha] = body
	e.reg.mu.Unlock()
	return sha
}

func (e *luaExec) lookup(sha string) (string, bool) {
	e.reg.mu.Lock()
	defer e.reg.mu.Unlock()
	body, ok := e.reg.scripts[strings.ToLower(sha)]
	return body, ok
}

func wrongArgs(cmd string) protocol.Value {
	return errValueStr(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd))
}

func (e *luaExec) handleEval(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgs("eval")
	}
	script, ok := argString(args[0])
	if !ok {
		return wrongArgs("eval")
	}
	keys, argv, errVal, ok := splitKeysArgv(args[1:])
	if !ok {
		return errVal
	}
	return e.run(ctx, script, keys, argv)
}

func (e *luaExec) handleEvalSHA(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgs("evalsha")
	}
	sha, ok := argString(args[0])
	if !ok {
		return wrongArgs("evalsha")
	}
	body, found := e.lookup(sha)
	if !found {
		return errValueStr("NOSCRIPT No matching script. Please use EVAL.")
	}
	keys, argv, errVal, ok := splitKeysArgv(args[1:])
	if !ok {
		return errVal
	}
	return e.run(ctx, body, keys, argv)
}

// splitKeysArgv 解析 [numkeys key... arg...]；失败回 errVal。
func splitKeysArgv(args []protocol.Value) (keys, argv []string, errVal protocol.Value, ok bool) {
	nStr, ok := argString(args[0])
	if !ok {
		return nil, nil, wrongArgs("eval"), false
	}
	n, err := strconv.Atoi(nStr)
	if err != nil {
		return nil, nil, errValueStr("ERR value is not an integer or out of range"), false
	}
	if n < 0 {
		return nil, nil, errValueStr("ERR Number of keys can't be negative"), false
	}
	if n > len(args)-1 {
		return nil, nil, errValueStr("ERR Number of keys can't be greater than number of args"), false
	}
	rest := args[1:]
	keys = make([]string, 0, n)
	for _, v := range rest[:n] {
		s, ok := argString(v)
		if !ok {
			return nil, nil, wrongArgs("eval"), false
		}
		keys = append(keys, s)
	}
	for _, v := range rest[n:] {
		s, ok := argString(v)
		if !ok {
			return nil, nil, wrongArgs("eval"), false
		}
		argv = append(argv, s)
	}
	return keys, argv, protocol.Value{}, true
}

func (e *luaExec) handleScript(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return wrongArgs("script")
	}
	sub, ok := argString(args[0])
	if !ok {
		return wrongArgs("script")
	}
	switch strings.ToUpper(sub) {
	case "LOAD":
		if len(args) < 2 {
			return wrongArgs("script")
		}
		body, _ := argString(args[1])
		return protocol.BulkOf(e.cached(body))
	case "EXISTS":
		out := make([]protocol.Value, 0, len(args)-1)
		for _, v := range args[1:] {
			s, ok := argString(v)
			_, found := e.lookup(s)
			if !ok || !found {
				out = append(out, protocol.Value{Kind: protocol.KindInteger, I: 0})
				continue
			}
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: 1})
		}
		return protocol.ArrayOf(out...)
	case "FLUSH":
		e.reg.mu.Lock()
		e.reg.scripts = make(map[string]string)
		e.reg.mu.Unlock()
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	case "KILL":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'script|kill' command")
		}
		killed, unkillable := e.reg.kill()
		switch {
		case unkillable:
			return errValueStr("UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.")
		case killed:
			return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
		default:
			return errValueStr("NOTBUSY No scripts in execution right now.")
		}
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s'. Try SCRIPT HELP.", sub))
	}
}

// run 编译并执行脚本，全程同 ctx（redis.call 内调共享 ctx）。
func (e *luaExec) run(ctx context.Context, body string, keys, argv []string) protocol.Value {
	sha := e.cached(body)
	L := lua.NewState()
	defer L.Close()
	// WithCancel 为底（kill 随时可中断），限时再包一层 WithTimeout；0 表示不限。
	tctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if e.timeout > 0 {
		var tc context.CancelFunc
		tctx, tc = context.WithTimeout(tctx, e.timeout)
		defer tc()
	}
	L.SetContext(tctx)

	L.SetGlobal("KEYS", strSliceTable(L, keys))
	L.SetGlobal("ARGV", strSliceTable(L, argv))
	rr := &luaRun{cancel: cancel}
	e.registerRedisLib(L, ctx, rr)
	registerCjsonLib(L)

	fn, err := L.Load(strings.NewReader(body), "user_script")
	if err != nil {
		return errValueStr("ERR Error compiling script (new function): " + compileDetail(body, err))
	}
	e.reg.track(rr)
	defer e.reg.untrack(rr)
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		if e.reg.wasKilled(rr) {
			return errValueStr("ERR Script killed by user with SCRIPT KILL... script: " + sha + ", on @user_script:1.")
		}
		if cemsg, ok := cjsonErrFrom(err); ok {
			return errValueStr(fmt.Sprintf("ERR %s script: %s, on @user_script:1.", cemsg, sha))
		}
		msg, _, _ := strings.Cut(err.Error(), "\n")
		return scriptError(msg, sha)
	}
	ret := L.Get(-1)
	L.Pop(1)
	v, err := luaToResp(ret)
	if err != nil {
		return scriptError(err.Error(), sha)
	}
	return v
}

// cjsonErrFrom 识别 cjson 原文错误（*cjsonErr，经 ApiError.Object 透出）：
// 真机格式为 `ERR <调用位><msg> script: ...`，此处同样组装（含调用位）。
func cjsonErrFrom(err error) (string, bool) {
	var apiErr *lua.ApiError
	if errors.As(err, &apiErr) {
		if ce, ok := apiErr.Object.(*cjsonErr); ok {
			msg, _, _ := strings.Cut(ce.msg, "\n")
			return ce.pos + msg, true
		}
	}
	return "", false
}

// scriptError 包运行时错误外层：自带 ERR /WRONGTYPE 前缀的不再补。
func scriptError(msg, sha string) protocol.Value {
	if !strings.HasPrefix(msg, "ERR ") && !strings.HasPrefix(msg, "WRONGTYPE") {
		msg = "ERR " + msg
	}
	return errValueStr(fmt.Sprintf("%s script: %s, on @user_script:1.", msg, sha))
}

// compileDetail 把 gopher-lua 编译错误重排为 `user_script:行: 信息`。四类可精确
// 映射为 Lua 5.1 原生措辞（7.2.6 探针逐字对齐），其余保留 gopher 原文。
func compileDetail(body string, err error) string {
	cause := err
	var apiErr *lua.ApiError
	if errors.As(err, &apiErr) && apiErr.Cause != nil {
		cause = apiErr.Cause
	}
	if cerr, ok := cause.(*lua.CompileError); ok {
		if m := gotoLabelRe.FindStringSubmatch(cerr.Error()); m != nil {
			return fmt.Sprintf("user_script:%s: '=' expected near '%s'", m[2], m[1])
		}
	}
	var perr *parse.Error
	if errors.As(cause, &perr) {
		line := perr.Pos.Line
		if line == parse.EOF {
			line = strings.Count(body, "\n") + 1
		}
		if mapped, ok := mapParseMessage(body, line, perr); ok {
			return mapped
		}
		return fmt.Sprintf("user_script:%d: %s", line, perr.Message)
	}
	msg := err.Error()
	if i := strings.Index(msg, "user_script"); i >= 0 {
		return strings.TrimRight(msg[i:], "\n")
	}
	return "user_script:1: " + msg
}

// gotoLabelRe 提取 gopher `no visible label 'X' for <goto> at line N` 的标签与行号。
var gotoLabelRe = regexp.MustCompile(`no visible label '([^']*)' for <goto> at line (\d+)`)

// mapParseMessage 把四类 gopher parse 错误改写为 Lua 5.1 原生措辞，ok=false
// 时调用方回退 gopher 原文。映射规则（7.2.6 探针）：
//   - 非法 16 进制：`malformed number near '<字面量>'`，字面量按错误列在正文定位后
//     向后吞 [0-9A-Za-z_.]（`0x`→`0x`，`0xG`→`0xG`）。
//   - 未闭合串：行尾/EOF→`near '<eof>'`；跨行→行号回退到起始引号行，
//     near 为原文照抄字面量套一层引号（`'abc`→`''abc'`）。
//   - 未闭合长注释：行号为正文末行，`unfinished long comment near '<eof>'`。
func mapParseMessage(body string, line int, perr *parse.Error) (string, bool) {
	switch perr.Message {
	case "illegal hexadecimal number":
		return fmt.Sprintf("user_script:%d: malformed number near '%s'",
			line, hexLiteral(body, line, perr.Pos.Column)), true
	case "unterminated string":
		if perr.Pos.Line == parse.EOF {
			return fmt.Sprintf("user_script:%d: unfinished string near '<eof>'", line), true
		}
		open := line - 1
		if q, ok := stringQuote(body, open, perr.Token); ok {
			return fmt.Sprintf("user_script:%d: unfinished string near '%s'",
				open, q+perr.Token), true
		}
		return "", false
	case "invalid multiline comment":
		return fmt.Sprintf("user_script:%d: unfinished long comment near '<eof>'", line), true
	}
	return "", false
}

// bodyLine 取正文第 n 行（1-based）。
func bodyLine(body string, n int) (string, bool) {
	if n < 1 {
		return "", false
	}
	lines := strings.Split(body, "\n")
	if n > len(lines) {
		return "", false
	}
	return lines[n-1], true
}

// hexLiteral 在错误行按列定位 `0x` 后吞出完整数字字面量；定位失败时退 Token。
func hexLiteral(body string, line, col int) string {
	if ln, ok := bodyLine(body, line); ok && col >= 1 {
		if i := col - 1; i+1 < len(ln) && ln[i] == '0' && (ln[i+1] == 'x' || ln[i+1] == 'X') {
			j := i + 2
			for j < len(ln) && isHexLitChar(ln[j]) {
				j++
			}
			return ln[i:j]
		}
		if i := strings.Index(ln, "0x"); i >= 0 {
			return hexExtend(ln, i)
		}
		if i := strings.Index(ln, "0X"); i >= 0 {
			return hexExtend(ln, i)
		}
	}
	return "0x"
}

func hexExtend(ln string, i int) string {
	j := i + 2
	for j < len(ln) && isHexLitChar(ln[j]) {
		j++
	}
	return ln[i:j]
}

func isHexLitChar(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '_' || c == '.'
}

// stringQuote 在起始行找 `'`/`"`+Token 确定字符串开引号；找不到时不映射。
func stringQuote(body string, openLine int, token string) (string, bool) {
	ln, ok := bodyLine(body, openLine)
	if !ok || token == "" {
		return "", false
	}
	if strings.Contains(ln, "'"+token) {
		return "'", true
	}
	if strings.Contains(ln, `"`+token) {
		return `"`, true
	}
	return "", false
}

func strSliceTable(L *lua.LState, ss []string) *lua.LTable {
	t := L.NewTable()
	for i, s := range ss {
		t.RawSetInt(i+1, lua.LString(s))
	}
	return t
}

func (e *luaExec) registerRedisLib(L *lua.LState, ctx context.Context, rr *luaRun) {
	lib := L.NewTable()
	L.SetFuncs(lib, map[string]lua.LGFunction{
		"call":         e.luaCall(ctx, false, rr),
		"pcall":        e.luaCall(ctx, true, rr),
		"sha1hex":      luaSha1hex,
		"log":          luaLog,
		"error_reply":  luaErrorReply,
		"status_reply": luaStatusReply,
	})
	for k, v := range map[string]int{
		"LOG_DEBUG": 0, "LOG_VERBOSE": 1, "LOG_NOTICE": 2, "LOG_WARNING": 3,
	} {
		lib.RawSetString(k, lua.LNumber(v))
	}
	L.SetGlobal("redis", lib)
}

func luaSha1hex(L *lua.LState) int {
	s := L.CheckString(1)
	L.Push(lua.LString(sha1Hex(s)))
	return 1
}

func luaLog(L *lua.LState) int {
	L.CheckInt(1)
	L.CheckString(2)
	L.Push(lua.LNil)
	return 1
}

func luaErrorReply(L *lua.LState) int {
	s := L.CheckString(1)
	t := L.NewTable()
	t.RawSetString("err", lua.LString("ERR "+s))
	L.Push(t)
	return 1
}

func luaStatusReply(L *lua.LState) int {
	s := L.CheckString(1)
	t := L.NewTable()
	t.RawSetString("ok", lua.LString(s))
	L.Push(t)
	return 1
}

// luaWriteCmds 复用写命令集合做 KILL 脏标记（包级单例，WriteCommandSet 每次新建 map）。
var luaWriteCmds = WriteCommandSet()

// luaNoScriptCmds 与真机 noscript 标记对齐：脚本内禁用（大小写不敏感）。
// 含 gedis 未实现的命令（QUIT/RESET/CONFIG 等）：查表先于 Handler，文案与真机一致。
var luaNoScriptCmds = map[string]struct{}{
	"SUBSCRIBE": {}, "UNSUBSCRIBE": {}, "PSUBSCRIBE": {}, "PUNSUBSCRIBE": {},
	"MONITOR": {}, "QUIT": {}, "RESET": {},
	"MULTI": {}, "EXEC": {}, "DISCARD": {}, "WATCH": {}, "UNWATCH": {},
	"EVAL": {}, "EVALSHA": {}, "SCRIPT": {},
	"CONFIG": {}, "DEBUG": {}, "SHUTDOWN": {}, "CLIENT": {}, "ACL": {},
}

// hasBlockOption 识别 XREAD 的 BLOCK 选项：STREAMS 之前的 BLOCK（大小写不敏感）
// 为选项，STREAMS 之后的是流名（真机按序解析，流可叫 block）。
func hasBlockOption(elems []protocol.Value) bool {
	for _, el := range elems {
		s := string(el.Bulk)
		if strings.EqualFold(s, "STREAMS") {
			return false
		}
		if strings.EqualFold(s, "BLOCK") {
			return true
		}
	}
	return false
}

// luaCall 实现 redis.call/pcall：直调 Router.Handler，错误在 call 下 raise、pcall 下装 {err} 表。
// 写命令实际分发即标脏（回复错误也算；未知命令/arity 等分发前拒绝的不脏）。
func (e *luaExec) luaCall(ctx context.Context, pcall bool, rr *luaRun) lua.LGFunction {
	return func(L *lua.LState) int {
		top := L.GetTop()
		nameV := L.Get(1)
		nameStr, ok := nameV.(lua.LString)
		if !ok {
			return e.raiseOrTable(L, pcall, "ERR Unknown Redis command called from script")
		}
		upName := strings.ToUpper(string(nameStr))
		elems := make([]protocol.Value, 0, top)
		for i := 2; i <= top; i++ {
			switch v := L.Get(i).(type) {
			case lua.LString:
				elems = append(elems, protocol.BulkOf(string(v)))
			case lua.LNumber:
				elems = append(elems, protocol.BulkOf(strconv.FormatInt(int64(v), 10)))
			default:
				return e.raiseOrTable(L, pcall, "ERR Lua redis() argument must be a string or number")
			}
		}
		if arity, known := CommandArity()[upName]; known {
			if !checkArity(arity, len(elems)+1) {
				return e.raiseOrTable(L, pcall, "ERR Wrong number of args calling Redis command from script")
			}
		}
		if _, denied := luaNoScriptCmds[upName]; denied {
			return e.raiseOrTable(L, pcall, "ERR This Redis command is not allowed from script")
		}
		if upName == "XREAD" && hasBlockOption(elems) {
			return e.raiseOrTable(L, pcall, "ERR "+string(nameStr)+" command is not allowed with BLOCK option from scripts")
		}
		h, ok := e.router.Handler(string(nameStr))
		if !ok {
			return e.raiseOrTable(L, pcall, "ERR Unknown Redis command called from script")
		}
		if luaWriteCmds[upName] {
			e.reg.markDirty(rr)
		}
		reply := h(ctx, elems)
		if reply.Kind == protocol.KindError && !pcall {
			// L.Error(level=0) 不补位置前缀；RaiseError 会补 `user_script:1: ` 从而偏离真机。
			L.Error(lua.LString(reply.S), 0)
			return 0
		}
		L.Push(respToLua(L, reply))
		return 1
	}
}

func (e *luaExec) raiseOrTable(L *lua.LState, pcall bool, msg string) int {
	if !pcall {
		L.Error(lua.LString(msg), 0)
		return 0
	}
	t := L.NewTable()
	t.RawSetString("err", lua.LString(msg))
	L.Push(t)
	return 1
}

// luaToResp 把脚本返回值转 RESP；嵌套表拍平（真机行为）。
func luaToResp(v lua.LValue) (protocol.Value, error) {
	switch t := v.(type) {
	case *lua.LNilType:
		return protocol.Value{Kind: protocol.KindBulkString}, nil
	case lua.LBool:
		if bool(t) {
			return protocol.Value{Kind: protocol.KindInteger, I: 1}, nil
		}
		return protocol.Value{Kind: protocol.KindBulkString}, nil
	case lua.LNumber:
		return protocol.Value{Kind: protocol.KindInteger, I: int64(t)}, nil
	case lua.LString:
		return protocol.BulkOf(string(t)), nil
	case *lua.LTable:
		if s, ok := t.RawGetString("err").(lua.LString); ok {
			return protocol.Value{Kind: protocol.KindError, S: string(s)}, nil
		}
		if s, ok := t.RawGetString("ok").(lua.LString); ok {
			return protocol.Value{Kind: protocol.KindSimpleString, S: string(s)}, nil
		}
		var out []protocol.Value
		for i := 1; ; i++ {
			item := t.RawGetInt(i)
			if _, isNil := item.(*lua.LNilType); isNil {
				break
			}
			cv, err := luaToResp(item)
			if err != nil {
				return protocol.Value{}, err
			}
			if cv.Kind == protocol.KindArray {
				out = append(out, cv.Elems...)
			} else {
				out = append(out, cv)
			}
		}
		if out == nil {
			out = []protocol.Value{}
		}
		return protocol.ArrayOf(out...), nil
	case *lua.LUserData:
		if t == cjsonNull {
			return protocol.Value{Kind: protocol.KindBulkString}, nil
		}
		return protocol.Value{}, fmt.Errorf("Lua redis() return value not convertible to RESP")
	case *cjsonErr:
		// pcall 捕获的 cjson 错误：真机按普通字符串返回。
		return protocol.BulkOf(t.msg), nil
	default:
		return protocol.Value{}, fmt.Errorf("Lua redis() return value not convertible to RESP")
	}
}

// respToLua 把 handler 回复转 Lua：status→{ok}、error→{err}、nil bulk→false、数组逐项。
func respToLua(L *lua.LState, v protocol.Value) lua.LValue {
	switch v.Kind {
	case protocol.KindInteger:
		return lua.LNumber(v.I)
	case protocol.KindDouble:
		return lua.LNumber(v.F)
	case protocol.KindBoolean:
		return lua.LBool(v.B)
	case protocol.KindSimpleString:
		t := L.NewTable()
		t.RawSetString("ok", lua.LString(v.S))
		return t
	case protocol.KindError:
		t := L.NewTable()
		t.RawSetString("err", lua.LString(v.S))
		return t
	case protocol.KindBulkString, protocol.KindBulkError, protocol.KindVerbatim:
		if v.Bulk == nil {
			return lua.LFalse
		}
		return lua.LString(string(v.Bulk))
	case protocol.KindArray, protocol.KindSet, protocol.KindPush:
		t := L.NewTable()
		for i, e := range v.Elems {
			if e.Kind == protocol.KindBulkString && e.Bulk == nil {
				t.RawSetInt(i+1, lua.LFalse)
				continue
			}
			t.RawSetInt(i+1, respToLua(L, e))
		}
		return t
	case protocol.KindMap, protocol.KindAttribute:
		t := L.NewTable()
		n := 0
		for _, p := range v.Pairs {
			n++
			t.RawSetInt(n, respToLua(L, p.K))
			n++
			t.RawSetInt(n, respToLua(L, p.V))
		}
		return t
	default:
		return lua.LNil
	}
}
