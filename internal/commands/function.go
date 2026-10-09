// FUNCTION/FCALL 命令族（Phase 6）。语义按 2026-10 真机 redis 7.2.6 探针对齐：
//   - FUNCTION LOAD：#! 头行解析 → 编译(user_function chunk) → PCall 收集注册 →
//     冲突检查 → 持久化 kv(fn:<库名>=源码)；注册期错误包
//     `ERR Error registering functions: ERR <首行>`，编译错
//     `ERR Error compiling function: <compileDetail>`。
//   - FUNCTION LIST/STATS/DELETE/FLUSH/HELP；KILL/DUMP/RESTORE 属二批未实现
//     （走 unknown subcommand，HELP 文案仍含）。
//   - FCALL/FCALL_RO：lower 索引查函数 → numkeys 解析 → RO flag 检查 → 每次
//     重载库代码收集 callback 后 PCall(2,1)；错误外层
//     `<msg> script: <fn>, on @user_function:<line>.`（行号取 traceback 首个
//     user_function:N，error()/RaiseError 则取首行前缀）。
//   - FCALL 走独立 LuaRegistry（fnReg）：SCRIPT KILL 不波及 FCALL。
package commands

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	lua "github.com/yuin/gopher-lua"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// fnMeta FUNCTION/FCALL 族 ACL 元数据（scripting 类；FUNCTION 无 key）。
var fnMeta = []acl.Meta{
	{Name: "FCALL", Category: "scripting", Keys: acl.KeySpec{Custom: acl.NumkeysKeys(1, 2)}},
	{Name: "FCALL_RO", Category: "scripting", ReadOnly: true, Keys: acl.KeySpec{Custom: acl.NumkeysKeys(1, 2)}},
	{Name: "FUNCTION", Category: "scripting", Keys: acl.KeySpec{First: -1}},
}

// fnKeyPrefix 持久化键：fn:<库名> → 库源码原文。
const fnKeyPrefix = "fn:"

func fnNullVal() protocol.Value { return protocol.Value{Kind: protocol.KindBulkString} }

func fnInt(n int) protocol.Value {
	return protocol.Value{Kind: protocol.KindInteger, I: int64(n)}
}

// RegisterFunctions 注册 FUNCTION/FCALL/FCALL_RO 并恢复 kv 中已持久化的库。
func RegisterFunctions(r *network.Router, kv KV, timeout time.Duration) {
	for _, m := range fnMeta {
		acl.RegisterMeta(m)
	}
	h := &fnHandler{
		store: &functionStore{kv: kv, libs: map[string]*fnLibrary{}, fns: map[string]fnRef{}},
		exec:  &luaExec{router: r, reg: &LuaRegistry{scripts: map[string]string{}}, timeout: timeout},
	}
	r.Register("FUNCTION", h.handleFunction)
	r.Register("FCALL", h.handleFcall(false))
	r.Register("FCALL_RO", h.handleFcall(true))
	h.store.restore(context.Background())
}

// ---------------- 数据模型 ----------------

// fnDef 是一条注册函数的元数据；callback 不入册（LOAD 态瞬态，FCALL 时重收集）。
type fnDef struct {
	name    string   // 注册时原样（LIST 展示保留大小写）
	desc    string   // description（hasDesc=false 时忽略）
	hasDesc bool
	flags   []string // 注册原文（大小写保留）
}

// hasNoWrites 判 no-writes flag（大小写不敏感）。
func (d *fnDef) hasNoWrites() bool {
	for _, f := range d.flags {
		if strings.EqualFold(f, "no-writes") {
			return true
		}
	}
	return false
}

// fnLibrary 一个 Lua 库：名字（精确大小写）+ 源码原文 + 注册序函数。
type fnLibrary struct {
	name  string
	code  string
	funcs []*fnDef
}

// fnRef 函数名（lower）→ 所属库与定义。
type fnRef struct {
	libName string
	lib     *fnLibrary
	def     *fnDef
}

// functionStore 内存注册表 + kv 持久；libs 精确名索引（DELETE 大小写敏感），
// fns lower 索引（FCALL 大小写不敏感、跨库查重）。
type functionStore struct {
	mu   sync.RWMutex
	kv   KV
	libs map[string]*fnLibrary
	fns  map[string]fnRef
}

type fnHandler struct {
	store *functionStore
	exec  *luaExec
}

// ---------------- 注册器（redis.register_function） ----------------

// fnCollector 收集一次库代码执行中的注册调用。错误统一 L.Error(msg,0)（无位置
// 前缀，由外层 `ERR Error registering functions: ` + ensureERR 包装）。
type fnCollector struct {
	defs []*fnDef
	seen map[string]struct{} // lower(name)，库内查重
	cbs  map[string]*lua.LFunction // 非 nil 时收集 callback（FCALL 重载用）
}

func newFnCollector(withCbs bool) *fnCollector {
	c := &fnCollector{seen: map[string]struct{}{}}
	if withCbs {
		c.cbs = map[string]*lua.LFunction{}
	}
	return c
}

// fnNameString 函数名转换：string 原样、number 转十进制文本；其余类型不支持。
func fnNameString(v lua.LValue) (string, bool) {
	switch t := v.(type) {
	case lua.LString:
		return string(t), true
	case lua.LNumber:
		return strconv.FormatFloat(float64(t), 'f', -1, 64), true
	}
	return "", false
}

// register 实现 redis.register_function：单参表单 / 双参位置形式。
// 表单校验顺序：name（缺/类型错→name 错；空/非法字符→fnNameErr）→ callback →
// flags → 库内查重 → description（真机探针：空表报 name 错、缺 callback 报
// callback 错、name 数字转串、description 缺省→null、''→空 bulk、数字→"42"）。
func (c *fnCollector) register(L *lua.LState) int {
	top := L.GetTop()
	var nameV, cbV, flagsV, descV lua.LValue
	tableForm := false
	switch {
	case top == 1:
		tbl, ok := L.Get(1).(*lua.LTable)
		if !ok {
			L.Error(lua.LString("calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments)."), 0)
			return 0
		}
		tableForm = true
		nameV = tbl.RawGetString("function_name")
		cbV = tbl.RawGetString("callback")
		flagsV = tbl.RawGetString("flags")
		descV = tbl.RawGetString("description")
	case top == 2:
		nameV = L.Get(1)
		cbV = L.Get(2)
	default:
		L.Error(lua.LString("wrong number of arguments to redis.register_function"), 0)
		return 0
	}

	// name：缺失/类型不支持 → name 错；空串或非法字符 → 库名 regex 文案（真机：''/hyphen 均报 fnNameErr）。
	name, ok := fnNameString(nameV)
	if !ok {
		L.Error(lua.LString("redis.register_function must get a function name argument"), 0)
		return 0
	}
	if !fnNameRe.MatchString(name) {
		L.Error(lua.LString(fnNameErr), 0)
		return 0
	}
	// callback：缺失与非函数分开报（真机探针）。
	if _, isNil := cbV.(*lua.LNilType); isNil {
		L.Error(lua.LString("redis.register_function must get a callback argument"), 0)
		return 0
	}
	cb, isFn := cbV.(*lua.LFunction)
	if !isFn {
		L.Error(lua.LString("second argument to redis.register_function must be a function"), 0)
		return 0
	}

	var flags []string
	if tableForm {
		if _, isNil := flagsV.(*lua.LNilType); !isNil {
			ft, isTbl := flagsV.(*lua.LTable)
			if !isTbl {
				L.Error(lua.LString("flags argument to redis.register_function must be a table representing function flags"), 0)
				return 0
			}
			for i := 1; ; i++ {
				item := ft.RawGetInt(i)
				if _, nilItem := item.(*lua.LNilType); nilItem {
					break
				}
				fs, isStr := item.(lua.LString)
				if !isStr || !validFnFlag(string(fs)) {
					L.Error(lua.LString("unknown flag given"), 0)
					return 0
				}
				flags = append(flags, string(fs))
			}
		}
	}

	// 库内查重（lower）。
	key := strings.ToLower(name)
	if _, dup := c.seen[key]; dup {
		L.Error(lua.LString("Function already exists in the library"), 0)
		return 0
	}
	c.seen[key] = struct{}{}

	d := &fnDef{name: name, flags: flags}
	if tableForm {
		if _, isNil := descV.(*lua.LNilType); !isNil {
			switch t := descV.(type) {
			case lua.LString:
				d.desc, d.hasDesc = string(t), true
			case lua.LNumber:
				d.desc = strconv.FormatFloat(float64(t), 'f', -1, 64)
				d.hasDesc = true
			}
		}
	}
	c.defs = append(c.defs, d)
	if c.cbs != nil {
		c.cbs[key] = cb
	}
	return 0
}

// validFnFlag 合法 flag 集（真机 7.2.6：no-writes/allow-oom/allow-stale）。
func validFnFlag(f string) bool {
	switch strings.ToLower(f) {
	case "no-writes", "allow-oom", "allow-stale":
		return true
	}
	return false
}

// ---------------- FUNCTION LOAD ----------------

// fnNameRe 库名/元数据 name 合法字符：字母数字下划线，至少一位。
var fnNameRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// fnNameErr 库名非法文案（真机原文）。
const fnNameErr = "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long"

// parseFunctionHeader 校验首行 #! 元数据并返回 (库名, 注释后可编译 body, 错误值)。
// 失败第三返回值非 nil。首行必须以 #! 开头；engine 到首空白且须为 lua（大小写
// 不敏感）；余下 token 只接受 name=<合法值>。body 把首行整体注释（-- 前缀）以
// 保持后续行号（编译错/运行时错误的 user_function:N 指向原始行）。
func parseFunctionHeader(code string) (string, string, *protocol.Value) {
	firstLine, rest, hasNL := strings.Cut(code, "\n")
	if !strings.HasPrefix(firstLine, "#!") {
		v := errValueStr("ERR Missing library metadata")
		return "", "", &v
	}
	// 真机：#! 开头但整串无换行（头行未终结）→ Invalid library metadata，先于 engine/token 解析。
	if !hasNL {
		v := errValueStr("ERR Invalid library metadata")
		return "", "", &v
	}
	meta := firstLine[2:]
	idx := strings.IndexFunc(meta, unicode.IsSpace)
	var engine, rem string
	if idx < 0 {
		engine, rem = meta, ""
	} else {
		engine, rem = meta[:idx], meta[idx:]
	}
	nameSeen := false
	nameVal := ""
	for _, tok := range strings.Fields(rem) {
		key, val, hasEq := strings.Cut(tok, "=")
		if hasEq && strings.EqualFold(key, "name") {
			if nameSeen {
				v := errValueStr("ERR Invalid metadata value, name argument was given multiple times")
				return "", "", &v
			}
			if !fnNameRe.MatchString(val) {
				v := errValueStr(fnNameErr)
				return "", "", &v
			}
			nameSeen = true
			nameVal = val
			continue
		}
		v := errValueStr("ERR Invalid metadata value given: " + tok)
		return "", "", &v
	}
	if !nameSeen {
		v := errValueStr("ERR Library name was not given")
		return "", "", &v
	}
	// 真机 probe18：engine 校验最后——token/名称错误都先于 Engine not found 报出。
	if !strings.EqualFold(engine, "lua") {
		v := errValueStr("ERR Engine '" + engine + "' not found")
		return "", "", &v
	}
	body := "--" + firstLine
	if hasNL {
		body += "\n" + rest
	}
	return nameVal, body, nil
}

// ensureERR 补 ERR 前缀（WRONGTYPE/自带 ERR 的不补）——与 scriptError 同规则。
func ensureERR(msg string) string {
	if strings.HasPrefix(msg, "ERR ") || strings.HasPrefix(msg, "WRONGTYPE") {
		return msg
	}
	return "ERR " + msg
}

// sanitizeErrText 净化错误文本（probe15/16 真机钉死）：先剥尾部 \r\n，
// 再把内部 \r、\n 各替换单空格。用于 Unknown option 与 {err}/error_reply 路径。
// 不用于 script 错误、ERR Unknown argument 等（真机原样保留换行）。
func sanitizeErrText(s string) string {
	s = strings.TrimRight(s, "\r\n")
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}

// sanitizeSub 仅替换不剥尾：unknown subcommand 路径（真机 `foo\n` →
// `foo ` 保留尾空格，probe16）。
func sanitizeSub(s string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}

// compileLibrary 编译+收集，成功返回 (lib, zero, true)；失败第三值 false 且
// 第二值为真机同形协议错误（LOAD 直接转发，restore 丢弃）。
func compileLibrary(code string) (*fnLibrary, protocol.Value, bool) {
	libName, body, fail := parseFunctionHeader(code)
	if fail != nil {
		return nil, *fail, false
	}
	L := lua.NewState()
	defer L.Close()
	c := newFnCollector(false)
	setupLoadEnv(L, c)
	fn, err := L.Load(strings.NewReader(body), "user_function")
	if err != nil {
		return nil, errValueStr("ERR Error compiling function: " + compileDetail(body, err, "user_function")), false
	}
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		first, _, _ := strings.Cut(err.Error(), "\n")
		return nil, errValueStr("ERR Error registering functions: " + ensureERR(first)), false
	}
	if len(c.defs) == 0 {
		return nil, errValueStr("ERR No functions registered"), false
	}
	return &fnLibrary{name: libName, code: code, funcs: c.defs}, protocol.Value{}, true
}

// setupLoadEnv 搭 LOAD 期沙箱：空 backup proxy（访问任何全局 → sandboxIndex
// 报 nonexistent global）+ redis 表（register_function/log/LOG_*，缺键经
// redis 表 __index 同样报 nonexistent global——真机 'call'/'pcall' 探针）。
func setupLoadEnv(L *lua.LState, c *fnCollector) {
	backup := L.NewTable()
	redisT := L.NewTable()
	redisT.RawSetString("register_function", L.NewFunction(c.register))
	redisT.RawSetString("log", L.NewFunction(luaLog))
	for k, v := range map[string]int{
		"LOG_DEBUG": 0, "LOG_VERBOSE": 1, "LOG_NOTICE": 2, "LOG_WARNING": 3,
	} {
		redisT.RawSetString(k, lua.LNumber(v))
	}
	empty := L.NewTable()
	rmt := L.NewTable()
	rmt.RawSetString("__index", L.NewClosure(sandboxIndex, empty))
	L.SetMetatable(redisT, rmt)

	proxy := L.NewTable()
	proxy.RawSetString("redis", redisT)
	mt := L.NewTable()
	mt.RawSetString("__index", L.NewClosure(sandboxIndex, backup))
	mt.RawSetString("__newindex", L.NewFunction(sandboxReadonly))
	mt.RawSetString("__metatable", L.NewTable())
	L.SetMetatable(proxy, mt)
	L.G.Global = proxy
	L.Env = proxy
}

// commitLocked 入册（调用方持写锁）。replace 时先摘除旧库自身函数索引；
// persist 控制是否回写 kv（restore 已在 kv 中，不回写）。
func (fs *functionStore) commitLocked(lib *fnLibrary, replace, persist bool) {
	if replace {
		if old, ok := fs.libs[lib.name]; ok {
			for _, d := range old.funcs {
				k := strings.ToLower(d.name)
				if ref, ok2 := fs.fns[k]; ok2 && ref.libName == lib.name {
					delete(fs.fns, k)
				}
			}
		}
	}
	fs.libs[lib.name] = lib
	for _, d := range lib.funcs {
		fs.fns[strings.ToLower(d.name)] = fnRef{libName: lib.name, lib: lib, def: d}
	}
	if persist && fs.kv != nil {
		_ = fs.kv.Set(context.Background(), []byte(fnKeyPrefix+lib.name), []byte(lib.code))
	}
}

// restore 启动恢复：Scan fn: 前缀重编译入册；编译失败跳过（不回写）。
func (fs *functionStore) restore(ctx context.Context) {
	if fs.kv == nil {
		return
	}
	keys, err := fs.kv.Scan(ctx, []byte(fnKeyPrefix))
	if err != nil {
		return
	}
	for _, k := range keys {
		code, err := fs.kv.Get(ctx, k)
		if err != nil {
			continue
		}
		lib, _, ok := compileLibrary(string(code))
		if !ok {
			continue
		}
		fs.mu.Lock()
		fs.commitLocked(lib, true, false)
		fs.mu.Unlock()
	}
}

// load 实现 FUNCTION LOAD [REPLACE] <code>。
func (h *fnHandler) load(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return wrongArgs("function|load")
	}
	var codeStr string
	replace := false
	if len(args) == 2 {
		s, ok := argString(args[1])
		if !ok {
			return wrongArgs("function|load")
		}
		codeStr = s
	} else {
		opt, ok := argString(args[1])
		if !ok {
			return wrongArgs("function|load")
		}
		if !strings.EqualFold(opt, "REPLACE") {
			return errValueStr("ERR Unknown option given: " + sanitizeErrText(opt))
		}
		replace = true
		s, ok := argString(args[2])
		if !ok {
			return wrongArgs("function|load")
		}
		codeStr = s
		if len(args) > 3 {
			extra, _ := argString(args[3])
			return errValueStr("ERR Unknown option given: " + sanitizeErrText(extra))
		}
	}
	lib, errVal, ok := compileLibrary(codeStr)
	if !ok {
		return errVal
	}
	// 冲突检查 + 入册（单锁内，防 TOCTOU）。
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if _, exists := h.store.libs[lib.name]; exists && !replace {
		return errValueStr("ERR Library '" + lib.name + "' already exists")
	}
	for _, d := range lib.funcs {
		if ref, found := h.store.fns[strings.ToLower(d.name)]; found {
			// REPLACE 重载同库时排除自身旧函数。
			if !(replace && ref.libName == lib.name) {
				return errValueStr("ERR Function " + d.name + " already exists")
			}
		}
	}
	h.store.commitLocked(lib, replace, true)
	return protocol.BulkOf(lib.name)
}

// ---------------- FUNCTION 子命令 ----------------

func (h *fnHandler) handleFunction(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return wrongArgs("function")
	}
	sub, ok := argString(args[0])
	if !ok {
		return wrongArgs("function")
	}
	switch strings.ToUpper(sub) {
	case "LOAD":
		return h.load(ctx, args)
	case "DELETE":
		if len(args) != 2 {
			return wrongArgs("function|delete")
		}
		return h.deleteLibrary(ctx, args[1])
	case "LIST":
		return h.list(args)
	case "STATS":
		if len(args) != 1 {
			return wrongArgs("function|stats")
		}
		return h.stats()
	case "FLUSH":
		return h.flush(ctx, args)
	case "HELP":
		if len(args) != 1 {
			return wrongArgs("function|help")
		}
		return functionHelp()
	}
	return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s'. Try FUNCTION HELP.", sanitizeSub(sub)))
}

// deleteLibrary DELETE：<库名> 精确大小写匹配，不存在报错。
func (h *fnHandler) deleteLibrary(ctx context.Context, v protocol.Value) protocol.Value {
	name, ok := argString(v)
	if !ok {
		return wrongArgs("function|delete")
	}
	h.store.mu.Lock()
	lib, exists := h.store.libs[name]
	if !exists {
		h.store.mu.Unlock()
		return errValueStr("ERR Library not found")
	}
	delete(h.store.libs, name)
	for _, d := range lib.funcs {
		k := strings.ToLower(d.name)
		if ref, ok2 := h.store.fns[k]; ok2 && ref.libName == name {
			delete(h.store.fns, k)
		}
	}
	h.store.mu.Unlock()
	if h.store.kv != nil {
		_ = h.store.kv.Delete(ctx, []byte(fnKeyPrefix+name))
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

// flush FUNCTION FLUSH [SYNC|ASYNC]：清空全部库。
func (h *fnHandler) flush(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) > 2 {
		return errValueStr("ERR unknown subcommand or wrong number of arguments for 'FLUSH'. Try FUNCTION HELP.")
	}
	if len(args) == 2 {
		opt, _ := argString(args[1])
		if !strings.EqualFold(opt, "SYNC") && !strings.EqualFold(opt, "ASYNC") {
			return errValueStr("ERR FUNCTION FLUSH only supports SYNC|ASYNC option")
		}
	}
	h.store.mu.Lock()
	h.store.libs = map[string]*fnLibrary{}
	h.store.fns = map[string]fnRef{}
	h.store.mu.Unlock()
	if h.store.kv != nil {
		if keys, err := h.store.kv.Scan(ctx, []byte(fnKeyPrefix)); err == nil {
			for _, k := range keys {
				_ = h.store.kv.Delete(ctx, k)
			}
		}
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

// list FUNCTION LIST [LIBRARYNAME PATTERN] [WITHCODE]：重复选项与未知参数均报
// `ERR Unknown argument <原token>`；LIBRARYNAME 缺 pattern 报小写 l 文案；
// glob 双方 ToLower 后匹配（真机大小写不敏感）。
func (h *fnHandler) list(args []protocol.Value) protocol.Value {
	var pattern string
	hasPattern := false
	withCode := false
	seen := map[string]bool{}
	for i := 1; i < len(args); i++ {
		opt, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR Unknown argument ")
		}
		up := strings.ToUpper(opt)
		switch up {
		case "LIBRARYNAME":
			if seen[up] {
				return errValueStr("ERR Unknown argument " + opt)
			}
			seen[up] = true
			if i+1 >= len(args) {
				return errValueStr("ERR library name argument was not given")
			}
			pat, _ := argString(args[i+1])
			pattern, hasPattern = pat, true
			i++
		case "WITHCODE":
			if seen[up] {
				return errValueStr("ERR Unknown argument " + opt)
			}
			seen[up] = true
			withCode = true
		default:
			return errValueStr("ERR Unknown argument " + opt)
		}
	}
	h.store.mu.RLock()
	defer h.store.mu.RUnlock()
	// 空结果需 *0（非 nil → *-1）：真机 probe13 钉死。
	out := []protocol.Value{}
	for _, lib := range h.store.libs {
		if hasPattern && !matchPattern(strings.ToLower(pattern), strings.ToLower(lib.name)) {
			continue
		}
		fns := []protocol.Value{}
		for _, d := range lib.funcs {
			var desc protocol.Value
			if d.hasDesc {
				desc = protocol.BulkOf(d.desc)
			} else {
				desc = fnNullVal()
			}
			flagVals := []protocol.Value{}
			for _, f := range d.flags {
				flagVals = append(flagVals, protocol.Value{Kind: protocol.KindSimpleString, S: f})
			}
			fns = append(fns, protocol.ArrayOf(
				protocol.BulkOf("name"), protocol.BulkOf(d.name),
				protocol.BulkOf("description"), desc,
				protocol.BulkOf("flags"), protocol.ArrayOf(flagVals...),
			))
		}
		els := []protocol.Value{
			protocol.BulkOf("library_name"), protocol.BulkOf(lib.name),
			protocol.BulkOf("engine"), protocol.BulkOf("LUA"),
			protocol.BulkOf("functions"), protocol.ArrayOf(fns...),
		}
		if withCode {
			els = append(els, protocol.BulkOf("library_code"), protocol.BulkOf(lib.code))
		}
		out = append(out, protocol.ArrayOf(els...))
	}
	return protocol.ArrayOf(out...)
}

// stats FUNCTION STATS：running_script 恒 null（无 FUNCTION KILL 追踪）。
func (h *fnHandler) stats() protocol.Value {
	h.store.mu.RLock()
	nLibs, nFns := len(h.store.libs), len(h.store.fns)
	h.store.mu.RUnlock()
	return protocol.ArrayOf(
		protocol.BulkOf("running_script"), fnNullVal(),
		protocol.BulkOf("engines"),
		protocol.ArrayOf(
			protocol.BulkOf("LUA"),
			protocol.ArrayOf(
				protocol.BulkOf("libraries_count"), fnInt(nLibs),
				protocol.BulkOf("functions_count"), fnInt(nFns),
			),
		),
	)
}

// ---------------- FCALL ----------------

// handleFcall FCALL/FCALL_RO：查函数 → numkeys → RO flag（错误顺序真机对齐）。
func (h *fnHandler) handleFcall(ro bool) func(context.Context, []protocol.Value) protocol.Value {
	cmdName := "fcall"
	if ro {
		cmdName = "fcall_ro"
	}
	return func(ctx context.Context, args []protocol.Value) protocol.Value {
		if len(args) < 2 {
			return wrongArgs(cmdName)
		}
		name, ok := argString(args[0])
		if !ok {
			return wrongArgs(cmdName)
		}
		h.store.mu.RLock()
		ref, found := h.store.fns[strings.ToLower(name)]
		h.store.mu.RUnlock()
		if !found {
			return errValueStr("ERR Function not found")
		}
		keys, argv, errVal, ok := splitFcallKeys(args[1:])
		if !ok {
			return errVal
		}
		if ro && !ref.def.hasNoWrites() {
			return errValueStr("ERR Can not execute a script with write flag using *_ro command.")
		}
		return h.runFunction(ctx, ref, keys, argv, ro)
	}
}

// splitFcallKeys 解析 [numkeys key... arg...]；三错文案与真机对齐（Bad number /
// negative / greater），与 EVAL 的 value is not an integer 不同。
func splitFcallKeys(args []protocol.Value) (keys, argv []string, errVal protocol.Value, ok bool) {
	nStr, ok := argString(args[0])
	if !ok {
		return nil, nil, errValueStr("ERR Bad number of keys provided"), false
	}
	n, err := strconv.Atoi(nStr)
	if err != nil {
		return nil, nil, errValueStr("ERR Bad number of keys provided"), false
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
		s, _ := argString(v)
		keys = append(keys, s)
	}
	for _, v := range rest[n:] {
		s, _ := argString(v)
		argv = append(argv, s)
	}
	return keys, argv, protocol.Value{}, true
}

// runFunction 执行一个函数：重载库代码收集本状态 callback（FUNCTION LOAD 同一份
// 源码），置 register_function=nil 后 PCall(2,1)（多返回值只取首个——真机
// `return 1,2` → :1）。KEYS/ARGV 不设全局（走形参；hardenSandbox 下访问全局
// KEYS 抛 nonexistent global → pcall false,false）。readonly：FCALL_RO 或
// fn 带 no-writes 时 luaCall 内拦截写命令。
func (h *fnHandler) runFunction(ctx context.Context, ref fnRef, keys, argv []string, ro bool) protocol.Value {
	L := lua.NewState()
	defer L.Close()
	tctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if h.exec.timeout > 0 {
		var tc context.CancelFunc
		tctx, tc = context.WithTimeout(tctx, h.exec.timeout)
		defer tc()
	}
	L.SetContext(tctx)

	rr := &luaRun{cancel: cancel, fnRun: true, readonly: ro || ref.def.hasNoWrites()}
	h.exec.registerRedisLib(L, ctx, rr)
	registerCjsonLib(L, h.exec.reg.cjsonCfg())
	registerBitLib(L)
	registerCmsgpackLib(L)
	registerStructLib(L)
	hardenSandbox(L)

	// 临时挂 collector 重跑库代码收集 callback，随后置 nil（真机
	// type(redis.register_function) == "nil"）。
	c := newFnCollector(true)
	redisT, _ := L.GetGlobal("redis").(*lua.LTable)
	redisT.RawSetString("register_function", L.NewFunction(c.register))
	body := fnCommentBody(ref.lib.code)
	fn, err := L.Load(strings.NewReader(body), "user_function")
	if err != nil {
		return errValueStr("ERR Error compiling function: " + compileDetail(body, err, "user_function"))
	}
	h.exec.reg.track(rr)
	defer h.exec.reg.untrack(rr)
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		if cemsg, ok := cjsonErrFrom(err); ok {
			return errValueStr(fmt.Sprintf("ERR %s script: %s, on @user_function:1.", cemsg, ref.def.name))
		}
		return fnScriptError(err, ref.def.name)
	}
	redisT.RawSetString("register_function", lua.LNil)
	cb, ok := c.cbs[strings.ToLower(ref.def.name)]
	if !ok {
		return errValueStr("ERR Function not found")
	}
	L.Push(cb)
	L.Push(strSliceTable(L, keys))
	L.Push(strSliceTable(L, argv))
	if err := L.PCall(2, 1, nil); err != nil {
		if cemsg, ok := cjsonErrFrom(err); ok {
			return errValueStr(fmt.Sprintf("ERR %s script: %s, on @user_function:1.", cemsg, ref.def.name))
		}
		return fnScriptError(err, ref.def.name)
	}
	ret := L.Get(-1)
	L.Pop(1)
	v, err := luaToResp(ret)
	if err != nil {
		return fnScriptError(err, ref.def.name)
	}
	return v
}

// fnCommentBody 首行整体加 --（保持行号）；与 parseFunctionHeader 同规则。
func fnCommentBody(code string) string {
	firstLine, rest, hasNL := strings.Cut(code, "\n")
	body := "--" + firstLine
	if hasNL {
		body += "\n" + rest
	}
	return body
}

// fnUserLineRe 匹配首行位置前缀 user_function:N:（error()/RaiseError 场景）。
var fnUserLineRe = regexp.MustCompile(`^user_function:(\d+): `)
var fnUserLineAnyRe = regexp.MustCompile(`user_function:(\d+)`)

// fnScriptError FCALL 运行时错误外层：
// `<msg> script: <fn>, on @user_function:<line>.`
// 行号：首行带 user_function:N: 前缀则取之（rest 若剥前缀后以 ERR/WRONGTYPE
// 开头才剥——error() 场景保留 `user_function:2: bang` 全文）；首行无前缀
// （L.Error(msg,0)：写拦截/嵌套命令/回复错）则行号取完整错误 traceback 中
// 首个 user_function:N（traceback 只存在于 err.Error()，Object 已剥）。
// msg 用 Object 全文（真机保留 error() 多行消息，probe16），只在剥前缀
// 规则命中时才截到 rest。
func fnScriptError(err error, fn string) protocol.Value {
	errText := apiErrMessage(err)
	var msg string
	var line = 1
	first, _, _ := strings.Cut(errText, "\n")
	if m := fnUserLineRe.FindStringSubmatch(first); m != nil {
		line, _ = strconv.Atoi(m[1])
		rest := errText[len(m[0]):]
		if strings.HasPrefix(rest, "ERR ") || strings.HasPrefix(rest, "WRONGTYPE") {
			msg = rest
		} else {
			msg = errText
		}
	} else {
		msg = errText
		if m2 := fnUserLineAnyRe.FindStringSubmatch(err.Error()); m2 != nil {
			line, _ = strconv.Atoi(m2[1])
		}
	}
	msg = ensureERR(msg)
	return errValueStr(fmt.Sprintf("%s script: %s, on @user_function:%d.", msg, fn, line))
}

// ---------------- HELP ----------------

// functionHelpLines FUNCTION HELP 40 条（真机 probe5 原文，SimpleString 输出）。
var functionHelpLines = []string{
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

func functionHelp() protocol.Value {
	out := make([]protocol.Value, 0, len(functionHelpLines))
	for _, s := range functionHelpLines {
		out = append(out, protocol.Value{Kind: protocol.KindSimpleString, S: s})
	}
	return protocol.ArrayOf(out...)
}
