// Package commands 的 cjson 支持：Redis 脚本内置 cjson 库核心子集。
//
// 范围（M6-F1 #27 核心 scope + M6-F6 #33 配置函数）：cjson.encode /
// cjson.decode / cjson.null / cjson.new / _NAME / _VERSION，以及 4 个配置函数
// encode_max_depth / decode_max_depth / encode_number_precision /
// encode_sparse_array（跨 EVAL 经 LuaRegistry 持久化，new() 实例私有出厂默认）。
// 与真机 Redis 7.2.6 默认配置行为逐字对齐；不存在 encode_empty_table_as_object。
package commands

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	lua "github.com/yuin/gopher-lua"
)

// cjsonNull 是 cjson.null 单例：type()=userdata；encode→null；脚本
// return→nil bulk（真机行为）。指针比较识别，不可伪造（沙箱内无其他
// userdata 来源）。
var cjsonNull = &lua.LUserData{}

// cjsonSettings 承载可配置项（真机 lua-cjson 全局 settings）：跨 EVAL 持久化
// 经 LuaRegistry（全局表绑定 reg 全局实例，new() 实例绑定私有出厂默认）。
// 并发脚本共享同一 Registry，读写经 mu 串行化。
type cjsonSettings struct {
	mu          sync.Mutex
	maxDepth    int64 // encode_max_depth，默认 1000
	decDepth    int64 // decode_max_depth，默认 1000
	precision   int64 // encode_number_precision，默认 14
	sparseConv  bool  // encode_sparse_array 首参，默认 false
	sparseRatio int64 // 默认 2
	sparseMax   int64 // 默认 10
}

func defaultCjsonSettings() *cjsonSettings {
	return &cjsonSettings{maxDepth: 1000, decDepth: 1000, precision: 14, sparseRatio: 2, sparseMax: 10}
}

// snap 一次性快照（encode/decode 全程用同一份，避免中途被 setter 改写）。
type cjsonSnap struct {
	maxDepth    int64
	decDepth    int64
	precision   int
	sparseConv  bool
	sparseRatio int64
	sparseMax   int64
}

func (s *cjsonSettings) snapshot() cjsonSnap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cjsonSnap{maxDepth: s.maxDepth, decDepth: s.decDepth, precision: int(s.precision),
		sparseConv: s.sparseConv, sparseRatio: s.sparseRatio, sparseMax: s.sparseMax}
}

// registerCjsonLib 注册 cjson 全局表（encode/decode/null/new/_NAME/_VERSION）。

// cjsonErr 承载 cjson 原文错误：错误对象本身干净（pcall 捕获按普通字符串），
// run() 识别后按真机格式补 `ERR <调用位> ` 前缀（调用位抛出时经 Where(1) 捕获）。
type cjsonErr struct {
	pos string // 如 "user_script:1: "
	msg string
}

func (e *cjsonErr) String() string        { return e.msg }
func (e *cjsonErr) Type() lua.LValueType { return lua.LTString }

func cjsonRaise(L *lua.LState, msg string) int {
	L.Error(&cjsonErr{pos: L.Where(1) + " ", msg: msg}, 0)
	return 0
}
func registerCjsonLib(L *lua.LState, cfg *cjsonSettings) {
	lib := L.NewTable()
	L.SetFuncs(lib, map[string]lua.LGFunction{
		"encode":               mkCjsonEncode(cfg),
		"decode":               mkCjsonDecode(cfg),
		"new":                  cjsonNew,
		"encode_max_depth":     mkCjsonDepth(cfg, "encode_max_depth", true),
		"decode_max_depth":     mkCjsonDepth(cfg, "decode_max_depth", false),
		"encode_number_precision": mkCjsonPrecision(cfg),
		"encode_sparse_array":  mkCjsonSparse(cfg),
	})
	lib.RawSetString("null", cjsonNull)
	lib.RawSetString("_NAME", lua.LString("cjson"))
	lib.RawSetString("_VERSION", lua.LString("2.1.0"))
	L.SetGlobal("cjson", lib)
}

// cjsonNew 返回独立 cjson 实例：私有出厂默认配置（探针：不继承全局修改），
// 与全局表同函数集（错误文案中函数名亦相同）。
func cjsonNew(L *lua.LState) int {
	cfg := defaultCjsonSettings()
	t := L.NewTable()
	t.RawSetString("encode", L.NewFunction(mkCjsonEncode(cfg)))
	t.RawSetString("decode", L.NewFunction(mkCjsonDecode(cfg)))
	t.RawSetString("new", L.NewFunction(cjsonNew))
	t.RawSetString("encode_max_depth", L.NewFunction(mkCjsonDepth(cfg, "encode_max_depth", true)))
	t.RawSetString("decode_max_depth", L.NewFunction(mkCjsonDepth(cfg, "decode_max_depth", false)))
	t.RawSetString("encode_number_precision", L.NewFunction(mkCjsonPrecision(cfg)))
	t.RawSetString("encode_sparse_array", L.NewFunction(mkCjsonSparse(cfg)))
	t.RawSetString("null", cjsonNull)
	t.RawSetString("_NAME", lua.LString("cjson"))
	t.RawSetString("_VERSION", lua.LString("2.1.0"))
	L.Push(t)
	return 1
}

func mkCjsonEncode(cfg *cjsonSettings) lua.LGFunction {
	return func(L *lua.LState) int {
		if L.GetTop() < 1 {
			cjsonRaise(L, "bad argument #1 to 'encode' (expected 1 argument)")
			return 0
		}
		if _, isNil := L.Get(1).(*lua.LNilType); isNil {
			L.Push(lua.LNil)
			return 1
		}
		snap := cfg.snapshot()
		var sb strings.Builder
		if err := writeJSON(&sb, L.Get(1), snap, 0); err != nil {
			cjsonRaise(L, err.Error())
			return 0
		}
		L.Push(lua.LString(sb.String()))
		return 1
	}
}

func mkCjsonDecode(cfg *cjsonSettings) lua.LGFunction {
	return func(L *lua.LState) int {
		if L.GetTop() < 1 {
			cjsonRaise(L, "bad argument #1 to 'decode' (expected 1 argument)")
			return 0
		}
		var s string
		switch v := L.Get(1).(type) {
		case lua.LString:
			s = string(v)
		case lua.LNumber:
			// 真机经 luaL_checkstring 把数字转字符串后再解析。
			s = fmtNum(float64(v), 14)
		default:
			cjsonRaise(L, "bad argument #1 to 'decode' (string expected, got "+v.Type().String()+")")
			return 0
		}
		v, msg, ok := decodeJSON(s, cfg.snapshot().decDepth)
		if !ok {
			cjsonRaise(L, msg)
			return 0
		}
		L.Push(v)
		return 1
	}
}

// mkCjsonDepth 通用 depth setter/getter：无参或单 nil→getter；多于 1 参→#2
// too many；coerce 错→#1 number；range 错→#1（探针：range 违反恒报 #1）。
func mkCjsonDepth(cfg *cjsonSettings, fname string, isEncode bool) lua.LGFunction {
	return func(L *lua.LState) int {
		top := L.GetTop()
		if top > 1 {
			cjsonRaise(L, fmt.Sprintf("bad argument #2 to '%s' (found too many arguments)", fname))
			return 0
		}
		cfg.mu.Lock()
		defer cfg.mu.Unlock()
		cur := cfg.maxDepth
		if !isEncode {
			cur = cfg.decDepth
		}
		if top == 0 || L.Get(1) == lua.LNil {
			L.Push(lua.LNumber(cur))
			return 1
		}
		v, ok := cjsonCheckIntLocked(L, fname, 1)
		if !ok {
			return 0
		}
		if v < 1 || v > 2147483647 {
			cjsonRaise(L, "bad argument #1 to '"+fname+"' (expected integer between 1 and 2147483647)")
			return 0
		}
		if isEncode {
			cfg.maxDepth = v
		} else {
			cfg.decDepth = v
		}
		L.Push(lua.LNumber(v))
		return 1
	}
}

// cjsonCheckIntLocked 复刻 luaL_checkinteger（number 向零截断、string 经
// tonumber 转换，余下类型报 number expected）：调用方已持有 cfg.mu；cjsonRaise
// 经 L.Error 长跳，defer 的 Unlock 仍会执行，无死锁。
func cjsonCheckIntLocked(L *lua.LState, fname string, n int) (int64, bool) {
	v := L.Get(n)
	switch t := v.(type) {
	case lua.LNumber:
		return int64(t), true
	case lua.LString:
		if f, err := strconv.ParseFloat(strings.TrimSpace(string(t)), 64); err == nil {
			return int64(f), true
		}
	}
	cjsonRaise(L, fmt.Sprintf("bad argument #%d to '%s' (number expected, got %s)", n, fname, v.Type()))
	return 0, false
}

func mkCjsonPrecision(cfg *cjsonSettings) lua.LGFunction {
	const fname = "encode_number_precision"
	return func(L *lua.LState) int {
		top := L.GetTop()
		if top > 1 {
			cjsonRaise(L, fmt.Sprintf("bad argument #2 to '%s' (found too many arguments)", fname))
			return 0
		}
		cfg.mu.Lock()
		defer cfg.mu.Unlock()
		if top == 0 || L.Get(1) == lua.LNil {
			L.Push(lua.LNumber(cfg.precision))
			return 1
		}
		v, ok := cjsonCheckIntLocked(L, fname, 1)
		if !ok {
			return 0
		}
		if v < 1 || v > 14 {
			cjsonRaise(L, "bad argument #1 to '"+fname+"' (expected integer between 1 and 14)")
			return 0
		}
		cfg.precision = v
		L.Push(lua.LNumber(v))
		return 1
	}
}

// mkCjsonSparse 实现 encode_sparse_array：0 参→triple getter；首参 nil（单参）
// →getter；boolean→convert setter；string/number→invalid option 原文；其他类型
// →string expected。后参缺省（nil）保当前值；第 4 参→#4 too many。
func mkCjsonSparse(cfg *cjsonSettings) lua.LGFunction {
	const fname = "encode_sparse_array"
	return func(L *lua.LState) int {
		top := L.GetTop()
		if top > 3 {
			cjsonRaise(L, fmt.Sprintf("bad argument #4 to '%s' (found too many arguments)", fname))
			return 0
		}
		cfg.mu.Lock()
		defer cfg.mu.Unlock()
		if top == 0 {
			pushSparseTriple(L, cfg)
			return 3
		}
		a1 := L.Get(1)
		if _, isNil := a1.(*lua.LNilType); isNil && top == 1 {
			pushSparseTriple(L, cfg)
			return 3
		}
		switch t := a1.(type) {
		case lua.LBool:
			cfg.sparseConv = bool(t)
		case *lua.LNilType:
			// nil + 后参 = setter，只改非 nil 后参。
		case lua.LString:
			cjsonRaise(L, fmt.Sprintf("invalid option '%s'", string(t)))
			return 0
		case lua.LNumber:
			cjsonRaise(L, fmt.Sprintf("invalid option '%s'", t.String()))
			return 0
		default:
			cjsonRaise(L, fmt.Sprintf("bad argument #1 to '%s' (string expected, got %s)", fname, a1.Type()))
			return 0
		}
		if top >= 2 && L.Get(2) != lua.LNil {
			v, ok := cjsonCheckIntLocked(L, fname, 2)
			if !ok {
				return 0
			}
			if v < 0 || v > 2147483647 {
				cjsonRaise(L, "bad argument #1 to '"+fname+"' (expected integer between 0 and 2147483647)")
				return 0
			}
			cfg.sparseRatio = v
		}
		if top >= 3 && L.Get(3) != lua.LNil {
			v, ok := cjsonCheckIntLocked(L, fname, 3)
			if !ok {
				return 0
			}
			if v < 0 || v > 2147483647 {
				cjsonRaise(L, "bad argument #1 to '"+fname+"' (expected integer between 0 and 2147483647)")
				return 0
			}
			cfg.sparseMax = v
		}
		L.Push(lua.LBool(cfg.sparseConv))
		return 1
	}
}

func pushSparseTriple(L *lua.LState, cfg *cjsonSettings) {
	L.Push(lua.LBool(cfg.sparseConv))
	L.Push(lua.LNumber(cfg.sparseRatio))
	L.Push(lua.LNumber(cfg.sparseMax))
}

// fmtNum 按配置精度格式化浮点（默认 %.14g）：整数无小数点，大数转科学计数。
func fmtNum(f float64, prec int) string {
	return strconv.FormatFloat(f, 'g', prec, 64)
}

// writeJSON 编码 Lua 值；错误文案逐字复刻 lua-cjson（含英式 serialise）。
// level 为当前嵌套层（top-level 调用传 0，table/array 进位后与 maxDepth 比较）。
func writeJSON(sb *strings.Builder, v lua.LValue, snap cjsonSnap, level int64) error {
	switch t := v.(type) {
	case lua.LBool:
		if bool(t) {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case lua.LNumber:
		f := float64(t)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("Cannot serialise number: must not be NaN or Inf")
		}
		sb.WriteString(fmtNum(f, snap.precision))
	case lua.LString:
		writeJSONString(sb, string(t))
	case *lua.LTable:
		lv := level + 1
		if lv > snap.maxDepth {
			return fmt.Errorf("Cannot serialise, excessive nesting (%d)", lv)
		}
		return writeJSONObject(sb, t, snap, lv)
	default:
		if ud, ok := v.(*lua.LUserData); ok && ud == cjsonNull {
			sb.WriteString("null")
			return nil
		}
		return fmt.Errorf("Cannot serialise %s: type not supported", luaTypeName(v))
	}
	return nil
}

func luaTypeName(v lua.LValue) string {
	return v.Type().String()
}

func writeJSONString(sb *strings.Builder, s string) {
	sb.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			sb.WriteString("\\\"")
		case '\\':
			sb.WriteString("\\\\")
		case '\b':
			sb.WriteString("\\b")
		case '\f':
			sb.WriteString("\\f")
		case '\n':
			sb.WriteString("\\n")
		case '\r':
			sb.WriteString("\\r")
		case '\t':
			sb.WriteString("\\t")
		default:
			if c < 0x20 {
				fmt.Fprintf(sb, "\\u%04x", c)
			} else {
				sb.WriteByte(c) // UTF-8 多字节原样透传（探针实证）
			}
		}
	}
	sb.WriteByte('"')
}

// writeJSONObject 判定数组/对象/稀疏拒绝（探针锁定的 FINAL MODEL）：
// 键非全 ≥1 整数→对象（全键保留，数字键经配置精度字符串化）；空表→{}；
// maxIdx≤sparseMax→ARRAY（null 填洞，不看 ratio）；否则 ratio 门：
// ratio>0 且 maxIdx/count>ratio（STRICT）才算 sparse，否则 ARRAY；
// sparse+convert→对象；sparse+!convert→error，除非 depth≤5 且 maxSet==10
// 则回退对象（razor-edge：30+ 观测锁定，唯一反例 m100/d5/max10，
// 邻域 m100/d6+、m100/d5/max20、m100/d5/max5 全 err；未来 fuzz 若推翻此
// 微区，优先以新观测为准）。
func writeJSONObject(sb *strings.Builder, t *lua.LTable, snap cjsonSnap, level int64) error {
	maxIdx := 0
	count := 0
	isArray := true
	keys := tableKeys(t)
	if len(keys) == 0 {
		sb.WriteString("{}")
		return nil
	}
	for _, k := range keys {
		if n, ok := k.(lua.LNumber); ok && float64(n) >= 1 && float64(n) == math.Trunc(float64(n)) {
			if int(n) > maxIdx {
				maxIdx = int(n)
			}
			count++
			continue
		}
		isArray = false
	}
	if isArray && (int64(maxIdx) > snap.sparseMax) &&
		snap.sparseRatio > 0 && float64(maxIdx)/float64(count) > float64(snap.sparseRatio) {
		if !snap.sparseConv && (snap.maxDepth > 5 || snap.sparseMax != 10) {
			return fmt.Errorf("Cannot serialise table: excessively sparse array")
		}
		return writeJSONObjectAsObject(sb, t, keys, snap, level)
	}
	if isArray {
		sb.WriteByte('[')
		for i := 1; i <= maxIdx; i++ {
			if i > 1 {
				sb.WriteByte(',')
			}
			item := t.RawGetInt(i)
			if _, isNil := item.(*lua.LNilType); isNil {
				sb.WriteString("null")
				continue
			}
			if err := writeJSON(sb, item, snap, level); err != nil {
				return err
			}
		}
		sb.WriteByte(']')
		return nil
	}
	return writeJSONObjectAsObject(sb, t, keys, snap, level)
}

func writeJSONObjectAsObject(sb *strings.Builder, t *lua.LTable, keys []lua.LValue, snap cjsonSnap, level int64) error {
	sb.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		var ks string
		switch n := k.(type) {
		case lua.LString:
			ks = string(n)
		case lua.LNumber:
			ks = fmtNum(float64(n), snap.precision)
		default:
			return fmt.Errorf("Cannot serialise %s: table key must be a number or string", luaTypeName(k))
		}
		writeJSONString(sb, ks)
		sb.WriteByte(':')
		if err := writeJSON(sb, t.RawGet(k), snap, level); err != nil {
			return err
		}
	}
	sb.WriteByte('}')
	return nil
}

// tableKeys 按 Next 顺序取表键（多键对象与真机 hash 序的差异属已知事项，
// compat 只用单键对象）。
func tableKeys(t *lua.LTable) []lua.LValue {
	var out []lua.LValue
	k := lua.LValue(lua.LNil)
	for {
		nk, _ := t.Next(k)
		if nk == lua.LNil {
			break
		}
		out = append(out, nk)
		k = nk
	}
	return out
}

// ---- decode：手写递归下降，错误文案逐字节复刻 lua-cjson ----

type jerr struct{ msg string }

func jerror(expect, found string, charNo int) *jerr {
	return &jerr{fmt.Sprintf("Expected %s but found %s at character %d", expect, found, charNo)}
}

type jdec struct {
	s   string
	p   int // 下一个待读字节下标；字符号 = p+1
	max int64
}

func decodeJSON(s string, maxDepth int64) (lua.LValue, string, bool) {
	d := &jdec{s: s, max: maxDepth}
	v, e := d.parseValue("value", 0)
	if e != nil {
		return nil, e.msg, false
	}
	d.skipWS()
	if d.p < len(d.s) {
		e := jerror("the end", d.tokenName(), d.p+1)
		return nil, e.msg, false
	}
	return v, "", true
}

func (d *jdec) skipWS() {
	for d.p < len(d.s) {
		switch d.s[d.p] {
		case ' ', '\t', '\n', '\r':
			d.p++
		default:
			return
		}
	}
}

// tokenName 按首字节命名 token（分隔符位置报错用，不消费输入）。
func (d *jdec) tokenName() string {
	if d.p >= len(d.s) {
		return "T_END"
	}
	c := d.s[d.p]
	switch c {
	case '{':
		return "T_OBJ_BEGIN"
	case '}':
		return "T_OBJ_END"
	case '[':
		return "T_ARR_BEGIN"
	case ']':
		return "T_ARR_END"
	case ',':
		return "T_COMMA"
	case ':':
		return "T_COLON"
	case '"':
		return "T_STRING"
	case 't':
		if strings.HasPrefix(d.s[d.p:], "true") {
			return "T_BOOLEAN"
		}
		return "invalid token"
	case 'f':
		if strings.HasPrefix(d.s[d.p:], "false") {
			return "T_BOOLEAN"
		}
		return "invalid token"
	}
	if c == 'n' && strings.HasPrefix(d.s[d.p:], "null") {
		return "T_NULL"
	}
	// 数字与 +/- 走 strtod 式尝试：扫出合法前缀→T_NUMBER，扫不出→invalid
	// number（探针：[1 12x]→T_NUMBER、[1 +2]→T_NUMBER、[1 -]→invalid number）。
	if c == '-' || c == '+' || (c >= '0' && c <= '9') {
		if numLen(d.s, d.p) > 0 {
			return "T_NUMBER"
		}
		return "invalid number"
	}
	return "invalid token"
}

// numLen 扫描 JSON 数字前缀，返回消费字节数（0=非数字）：
// sign? intdigits* ('.' fracdigits*)? exponent?，要求整数部与小数部合计至少
// 1 个数字；指数残缺则回退不消费（"1e" 只吃 "1"）。探针：+.5→0.5、1.e2→100。
func numLen(s string, p int) int {
	i := p
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		i++
	}
	ds := i
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	hasInt := i > ds
	hasFrac := false
	if i < len(s) && s[i] == '.' {
		i++
		fs := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		hasFrac = i > fs
	}
	if !hasInt && !hasFrac {
		return 0
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		q := i + 1
		if q < len(s) && (s[q] == '-' || s[q] == '+') {
			q++
		}
		qs := q
		for q < len(s) && s[q] >= '0' && s[q] <= '9' {
			q++
		}
		if q > qs {
			i = q
		}
	}
	return i - p
}

func (d *jdec) parseValue(expect string, depth int64) (lua.LValue, *jerr) {
	d.skipWS()
	if d.p >= len(d.s) {
		return nil, jerror(expect, "T_END", d.p+1)
	}
	switch c := d.s[d.p]; {
	case c == '"':
		return d.parseString()
	case c == '{', c == '[':
		if lv := depth + 1; lv > d.max {
			return nil, &jerr{fmt.Sprintf("Found too many nested data structures (%d) at character %d", lv, d.p+1)}
		} else if c == '{' {
			return d.parseObject(lv)
		} else {
			return d.parseArray(lv)
		}
	case c == 't' && strings.HasPrefix(d.s[d.p:], "true"):
		d.p += 4
		return lua.LBool(true), nil
	case c == 'f' && strings.HasPrefix(d.s[d.p:], "false"):
		d.p += 5
		return lua.LBool(false), nil
	case c == 'n' && strings.HasPrefix(d.s[d.p:], "null"):
		d.p += 4
		return cjsonNull, nil
	case c == '-' || c == '+' || (c >= '0' && c <= '9'):
		return d.parseNumber()
	default:
		return nil, jerror(expect, d.tokenName(), d.p+1)
	}
}

// parseNumber 经 numLen 消费数字前缀后转 float64（溢出按 strconv.ErrRange 取 ±Inf）。
func (d *jdec) parseNumber() (lua.LValue, *jerr) {
	start := d.p
	n := numLen(d.s, d.p)
	if n == 0 {
		return nil, jerror("value", "invalid number", start+1)
	}
	d.p += n
	f, err := strconv.ParseFloat(d.s[start:d.p], 64)
	if err != nil {
		if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
			f, _ = strconv.ParseFloat(d.s[start:d.p], 64)
		} else {
			return nil, jerror("value", "invalid number", start+1)
		}
	}
	return lua.LNumber(f), nil
}

func (d *jdec) parseString() (lua.LValue, *jerr) {
	d.p++ // 跳过开引号；错误字符号按反斜杠位置/输入结尾+1（探针实证）
	var sb strings.Builder
	for {
		if d.p >= len(d.s) {
			return nil, jerror("value", "unexpected end of string", len(d.s)+1)
		}
		c := d.s[d.p]
		switch {
		case c == '"':
			d.p++
			return lua.LString(sb.String()), nil
		case c == '\\':
			esc := d.p
			d.p++
			if d.p >= len(d.s) {
				return nil, jerror("value", "unexpected end of string", len(d.s)+1)
			}
			e := d.s[d.p]
			switch e {
			case '"', '\\', '/':
				sb.WriteByte(e)
				d.p++
			case 'b':
				sb.WriteByte('\b')
				d.p++
			case 'f':
				sb.WriteByte('\f')
				d.p++
			case 'n':
				sb.WriteByte('\n')
				d.p++
			case 'r':
				sb.WriteByte('\r')
				d.p++
			case 't':
				sb.WriteByte('\t')
				d.p++
			case 'u':
				r, ok := d.parseUnicode(esc)
				if !ok {
					return nil, jerror("value", "invalid unicode escape code", esc+1)
				}
				sb.WriteRune(r)
			default:
				return nil, jerror("value", "invalid escape code", esc+1)
			}
		default:
			sb.WriteByte(c)
			d.p++
		}
	}
}

// parseUnicode 解析 \uXXXX（含代理对组合）；调用时 d.p 在 'u' 上。
func (d *jdec) parseUnicode(esc int) (rune, bool) {
	_ = esc
	hi, ok := d.hex4()
	if !ok {
		return 0, false
	}
	if hi >= 0xD800 && hi <= 0xDBFF {
		if d.p+1 < len(d.s) && d.s[d.p] == '\\' && d.s[d.p+1] == 'u' {
			d.p += 2
			lo, ok := d.hex4()
			if !ok || lo < 0xDC00 || lo > 0xDFFF {
				return 0, false
			}
			return rune(0x10000 + (hi-0xD800)<<10 + (lo - 0xDC00)), true
		}
		return 0, false
	}
	if hi >= 0xDC00 && hi <= 0xDFFF {
		return 0, false
	}
	return rune(hi), true
}

func (d *jdec) hex4() (int, bool) {
	if d.p+4 >= len(d.s) {
		return 0, false
	}
	v := 0
	for i := 1; i <= 4; i++ {
		c := d.s[d.p+i]
		var h int
		switch {
		case c >= '0' && c <= '9':
			h = int(c - '0')
		case c >= 'a' && c <= 'f':
			h = int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			h = int(c-'A') + 10
		default:
			return 0, false
		}
		v = v*16 + h
	}
	d.p += 5
	return v, true
}

func (d *jdec) parseObject(depth int64) (lua.LValue, *jerr) {
	d.p++ // '{'
	t := &lua.LTable{}
	d.skipWS()
	if d.p < len(d.s) && d.s[d.p] == '}' {
		d.p++
		return t, nil
	}
	for {
		d.skipWS()
		if d.p >= len(d.s) || d.s[d.p] != '"' {
			name := d.tokenName()
			return nil, jerror("object key string", name, d.p+1)
		}
		kv, e := d.parseString()
		if e != nil {
			return nil, e
		}
		d.skipWS()
		if d.p >= len(d.s) || d.s[d.p] != ':' {
			return nil, jerror("colon", d.tokenName(), d.p+1)
		}
		d.p++
		v, e := d.parseValue("value", depth)
		if e != nil {
			return nil, e
		}
		t.RawSetString(kv.(lua.LString).String(), v) // 重复键后赢（探针实证）
		d.skipWS()
		if d.p < len(d.s) && d.s[d.p] == ',' {
			d.p++
			continue
		}
		if d.p < len(d.s) && d.s[d.p] == '}' {
			d.p++
			return t, nil
		}
		return nil, jerror("comma or object end", d.tokenName(), d.p+1)
	}
}

func (d *jdec) parseArray(depth int64) (lua.LValue, *jerr) {
	d.p++ // '['
	t := &lua.LTable{}
	d.skipWS()
	if d.p < len(d.s) && d.s[d.p] == ']' {
		d.p++
		return t, nil
	}
	n := 0
	for {
		v, e := d.parseValue("value", depth)
		if e != nil {
			return nil, e
		}
		n++
		t.RawSetInt(n, v)
		d.skipWS()
		if d.p < len(d.s) && d.s[d.p] == ',' {
			d.p++
			continue
		}
		if d.p < len(d.s) && d.s[d.p] == ']' {
			d.p++
			return t, nil
		}
		return nil, jerror("comma or array end", d.tokenName(), d.p+1)
	}
}
