// Package commands 的 cjson 支持：Redis 脚本内置 cjson 库核心子集。
//
// 范围（M6-F1 #27 核心 scope）：cjson.encode / cjson.decode / cjson.null /
// cjson.new / _NAME / _VERSION，与真机 Redis 7.2.6 默认配置行为逐字对齐。
// 延后（已知差异）：7 个配置函数（encode_number_precision 等）与跨脚本
// settings 持久化——我方每 EVAL 新沙箱，行为恒等于真机"未调过配置"状态。
package commands

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

// cjsonNull 是 cjson.null 单例：type()=userdata；encode→null；脚本
// return→nil bulk（真机行为）。指针比较识别，不可伪造（沙箱内无其他
// userdata 来源）。
var cjsonNull = &lua.LUserData{}

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
func registerCjsonLib(L *lua.LState) {
	lib := L.NewTable()
	L.SetFuncs(lib, map[string]lua.LGFunction{
		"encode": cjsonEncode,
		"decode": cjsonDecode,
		"new":    cjsonNew,
	})
	lib.RawSetString("null", cjsonNull)
	lib.RawSetString("_NAME", lua.LString("cjson"))
	lib.RawSetString("_VERSION", lua.LString("2.1.0"))
	L.SetGlobal("cjson", lib)
}

// cjsonNew 返回独立 cjson 实例（核心 scope 下配置恒默认，与全局表同行为）。
func cjsonNew(L *lua.LState) int {
	t := L.NewTable()
	t.RawSetString("encode", L.NewFunction(cjsonEncode))
	t.RawSetString("decode", L.NewFunction(cjsonDecode))
	t.RawSetString("null", cjsonNull)
	t.RawSetString("_NAME", lua.LString("cjson"))
	t.RawSetString("_VERSION", lua.LString("2.1.0"))
	L.Push(t)
	return 1
}

func cjsonEncode(L *lua.LState) int {
	if L.GetTop() < 1 {
		cjsonRaise(L, "bad argument #1 to 'encode' (expected 1 argument)")
		return 0
	}
	if _, isNil := L.Get(1).(*lua.LNilType); isNil {
		L.Push(lua.LNil)
		return 1
	}
	var sb strings.Builder
	if err := writeJSON(&sb, L.Get(1)); err != nil {
		cjsonRaise(L, err.Error())
		return 0
	}
	L.Push(lua.LString(sb.String()))
	return 1
}

func cjsonDecode(L *lua.LState) int {
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
		s = fmtNum(float64(v))
	default:
		cjsonRaise(L, "bad argument #1 to 'decode' (string expected, got "+v.Type().String()+")")
		return 0
	}
	v, msg, ok := decodeJSON(s)
	if !ok {
		cjsonRaise(L, msg)
		return 0
	}
	L.Push(v)
	return 1
}

// fmtNum 复刻 lua-cjson 默认精度（%.14g）：整数无小数点，大数转科学计数。
func fmtNum(f float64) string {
	return strconv.FormatFloat(f, 'g', 14, 64)
}

// writeJSON 编码 Lua 值；错误文案逐字复刻 lua-cjson（含英式 serialise）。
func writeJSON(sb *strings.Builder, v lua.LValue) error {
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
		sb.WriteString(fmtNum(f))
	case lua.LString:
		writeJSONString(sb, string(t))
	case *lua.LTable:
		return writeJSONObject(sb, t)
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

// writeJSONObject 判定数组/对象：键全为 ≥1 整数→数组（1..max，洞补 null）；
// 否则对象（全键保留，数字键经 fmtNum 字符串化）；空表→{}（探针实证）。
func writeJSONObject(sb *strings.Builder, t *lua.LTable) error {
	maxIdx := 0
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
			continue
		}
		isArray = false
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
			if err := writeJSON(sb, item); err != nil {
				return err
			}
		}
		sb.WriteByte(']')
		return nil
	}
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
			ks = fmtNum(float64(n))
		default:
			return fmt.Errorf("Cannot serialise %s: table key must be a number or string", luaTypeName(k))
		}
		writeJSONString(sb, ks)
		sb.WriteByte(':')
		if err := writeJSON(sb, t.RawGet(k)); err != nil {
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
	s string
	p int // 下一个待读字节下标；字符号 = p+1
}

func decodeJSON(s string) (lua.LValue, string, bool) {
	d := &jdec{s: s}
	v, e := d.parseValue("value")
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

func (d *jdec) parseValue(expect string) (lua.LValue, *jerr) {
	d.skipWS()
	if d.p >= len(d.s) {
		return nil, jerror(expect, "T_END", d.p+1)
	}
	switch c := d.s[d.p]; {
	case c == '"':
		return d.parseString()
	case c == '{':
		return d.parseObject()
	case c == '[':
		return d.parseArray()
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

func (d *jdec) parseObject() (lua.LValue, *jerr) {
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
		v, e := d.parseValue("value")
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

func (d *jdec) parseArray() (lua.LValue, *jerr) {
	d.p++ // '['
	t := &lua.LTable{}
	d.skipWS()
	if d.p < len(d.s) && d.s[d.p] == ']' {
		d.p++
		return t, nil
	}
	n := 0
	for {
		v, e := d.parseValue("value")
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
