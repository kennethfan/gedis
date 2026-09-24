package commands

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

// registerCmsgpackLib 注册 cmsgpack 全局表（对标 lua-cmsgpack 0.4.0：
// pack/unpack/unpack_one/unpack_limit + _NAME/_VERSION，只读库）。
func registerCmsgpackLib(L *lua.LState) {
	lib := L.NewTable()
	L.SetFuncs(lib, map[string]lua.LGFunction{
		"pack":         cmsgpackPack,
		"unpack":       cmsgpackUnpack,
		"unpack_one":   cmsgpackUnpackOne,
		"unpack_limit": cmsgpackUnpackLimit,
	})
	lib.RawSetString("_NAME", lua.LString("cmsgpack"))
	lib.RawSetString("_VERSION", lua.LString("lua-cmsgpack 0.4.0"))
	L.SetGlobal("cmsgpack", lib)
}

// cmsgpackMaxNesting 为 pack 嵌套层数上限；达到该层数的 table 按 nil 编码
// （对标 lua-cmsgpack 的 LUACMSGPACK_MAX_NESTING=16，顶层入参 level=0）。
const cmsgpackMaxNesting = 16

// cmsgpackPack 拼接编码全部入参；零参按真机文案报错。
func cmsgpackPack(L *lua.LState) int {
	if L.GetTop() == 0 {
		libArgError(L, "pack", 0, "MessagePack pack needs input.")
		return 0
	}
	var out []byte
	for i := 1; i <= L.GetTop(); i++ {
		out = append(out, cmsgpackEncode(L.Get(i), 0)...)
	}
	L.Push(lua.LString(string(out)))
	return 1
}

// cmsgpackUnpack 解码全部值并返回（空串返回零值）。
func cmsgpackUnpack(L *lua.LState) int {
	s, ok := cmsgpackStringArg(L, "unpack", 1)
	if !ok {
		return 0
	}
	d := &cmsgpackDecoder{s: s}
	var vals []lua.LValue
	for d.pos < len(s) {
		v, ok := d.value(L)
		if !ok {
			return 0
		}
		vals = append(vals, v)
	}
	for _, v := range vals {
		L.Push(v)
	}
	return len(vals)
}

// cmsgpackUnpackOne 返回 (下个位置, 值)；无值可读时返回单个下个位置。
// 位置为 0 起始字节偏移；读尽时下个位置为 -1。多余参数忽略。
func cmsgpackUnpackOne(L *lua.LState) int {
	s, ok := cmsgpackStringArg(L, "unpack_one", 1)
	if !ok {
		return 0
	}
	off, ok := cmsgpackIntArg(L, "unpack_one", 2, 0, false)
	if !ok {
		return 0
	}
	return cmsgpackDecodeRange(L, s, off, 1)
}

// cmsgpackUnpackLimit 参数顺序 (str, limit [, offset])；limit 为解码值个数，
// limit 为 0 时走特殊形态：offset 为 0 返回全部值（不带位置），offset>0 返回首个值。
func cmsgpackUnpackLimit(L *lua.LState) int {
	s, ok := cmsgpackStringArg(L, "unpack_limit", 1)
	if !ok {
		return 0
	}
	limit, ok := cmsgpackIntArg(L, "unpack_limit", 2, 0, true)
	if !ok {
		return 0
	}
	off, ok := cmsgpackIntArg(L, "unpack_limit", 3, 0, false)
	if !ok {
		return 0
	}
	if off < 0 || limit < 0 {
		libErrorf(L, "Invalid request to unpack with offset of %d and limit of %d.", off, len(s))
		return 0
	}
	if off > len(s) {
		libErrorf(L, "Start offset %d greater than input length %d.", off, len(s))
		return 0
	}
	if limit == 0 {
		return cmsgpackDecodeAll(L, s, off)
	}
	return cmsgpackDecodeRange(L, s, off, limit)
}

// cmsgpackDecodeAll 解码全部值（limit==0 形态）：offset 为 0 返回所有值，
// offset>0 直接回显 offset（不解码）。
func cmsgpackDecodeAll(L *lua.LState, s string, off int) int {
	if off > 0 {
		L.Push(lua.LNumber(off))
		return 1
	}
	d := &cmsgpackDecoder{s: s}
	var vals []lua.LValue
	for d.pos < len(s) {
		v, ok := d.value(L)
		if !ok {
			return 0
		}
		vals = append(vals, v)
	}
	for _, v := range vals {
		L.Push(v)
	}
	return len(vals)
}

// cmsgpackDecodeRange 从字节偏移 off 起解码（limit<0 不限个数），返回
// (下个位置, 值...)；无值时返回单个下个位置（读尽为 -1）。
func cmsgpackDecodeRange(L *lua.LState, s string, off, limit int) int {
	if off < 0 {
		libErrorf(L, "Invalid request to unpack with offset of %d and limit of %d.", off, len(s))
		return 0
	}
	if off > len(s) {
		libErrorf(L, "Start offset %d greater than input length %d.", off, len(s))
		return 0
	}
	d := &cmsgpackDecoder{s: s, pos: off}
	var vals []lua.LValue
	for d.pos < len(s) && (limit < 0 || len(vals) < limit) {
		v, ok := d.value(L)
		if !ok {
			return 0
		}
		vals = append(vals, v)
	}
	if len(vals) == 0 {
		next := off
		if off >= len(s) {
			next = -1
		}
		L.Push(lua.LNumber(next))
		return 1
	}
	next := d.pos
	if next >= len(s) {
		next = -1
	}
	L.Push(lua.LNumber(next))
	for _, v := range vals {
		L.Push(v)
	}
	return len(vals) + 1
}

// cmsgpackStringArg 取串参数（对标 luaL_checklstring：string 直接用，number
// 按 Lua tostring 折；缺参报 "no value"，其余报类型名）。
func cmsgpackStringArg(L *lua.LState, fname string, n int) (string, bool) {
	if n > L.GetTop() {
		libArgError(L, fname, n, "string expected, got no value")
		return "", false
	}
	switch t := L.Get(n).(type) {
	case lua.LString:
		return string(t), true
	case lua.LNumber:
		return structNumString(float64(t)), true
	default:
		libArgError(L, fname, n, "string expected, got "+L.Get(n).Type().String())
		return "", false
	}
}

// cmsgpackIntArg 取整数参数（对标 luaL_optinteger/luaL_checkinteger：缺参或
// 显式 nil 取缺省（required 时报错）；number 向零截断；数字串按 tonumber 折）。
func cmsgpackIntArg(L *lua.LState, fname string, n, def int, required bool) (int, bool) {
	missing := func(got string) (int, bool) {
		if required {
			libArgError(L, fname, n, "number expected, got "+got)
			return 0, false
		}
		return def, true
	}
	if n > L.GetTop() {
		return missing("no value")
	}
	v := L.Get(n)
	if _, isNil := v.(*lua.LNilType); isNil {
		if required {
			return missing("nil")
		}
		return def, true
	}
	switch t := v.(type) {
	case lua.LNumber:
		return int(float64(t)), true
	case lua.LString:
		if f, ok := cmsgpackToNumber(string(t)); ok {
			return int(f), true
		}
		libArgError(L, fname, n, "number expected, got string")
		return 0, false
	default:
		libArgError(L, fname, n, "number expected, got "+v.Type().String())
		return 0, false
	}
}

// cmsgpackToNumber 复刻 Lua 5.1 tonumber（十进制与 0x 十六进制，前后空格容忍）。
func cmsgpackToNumber(s string) (float64, bool) {
	t := strings.TrimSpace(s)
	if t == "" {
		return 0, false
	}
	if len(t) > 2 && t[0] == '0' && (t[1] == 'x' || t[1] == 'X') {
		u, err := strconv.ParseUint(t[2:], 16, 64)
		if err != nil {
			return 0, false
		}
		return float64(u), true
	}
	f, err := strconv.ParseFloat(t, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// cmsgpackEncode 按 MessagePack 编码 Lua 值；未知类型与超深嵌套 table 编 nil。
func cmsgpackEncode(v lua.LValue, level int) []byte {
	switch t := v.(type) {
	case *lua.LNilType:
		return []byte{0xc0}
	case lua.LBool:
		if bool(t) {
			return []byte{0xc3}
		}
		return []byte{0xc2}
	case lua.LNumber:
		return cmsgpackEncodeNumber(float64(t))
	case lua.LString:
		return cmsgpackEncodeString(string(t))
	case *lua.LTable:
		if level == cmsgpackMaxNesting {
			return []byte{0xc0}
		}
		return cmsgpackEncodeTable(t, level)
	default:
		return []byte{0xc0}
	}
}

// cmsgpackEncodeNumber 按 0.4.0 分支编码整数；超出 int64 范围的大数走浮点分支。
func cmsgpackEncodeNumber(f float64) []byte {
	if !math.IsNaN(f) && !math.IsInf(f, 0) && math.Trunc(f) == f &&
		f >= -9223372036854775808.0 && f < 9223372036854775808.0 {
		i := int64(f)
		switch {
		case i >= 0 && i <= 0x7f:
			return []byte{byte(i)}
		case i >= -32 && i < 0:
			return []byte{byte(int8(i))}
		case i >= 0 && i <= 0xff:
			return []byte{0xcc, byte(i)}
		case i >= 0 && i <= 0xffff:
			b := []byte{0xcd, 0, 0}
			binary.BigEndian.PutUint16(b[1:], uint16(i))
			return b
		case i >= 0 && i <= 0xffffffff:
			b := make([]byte, 5)
			b[0] = 0xce
			binary.BigEndian.PutUint32(b[1:], uint32(i))
			return b
		case i >= 0:
			b := make([]byte, 9)
			b[0] = 0xcf
			binary.BigEndian.PutUint64(b[1:], uint64(i))
			return b
		case i >= -128:
			return []byte{0xd0, byte(int8(i))}
		case i >= -32768:
			b := []byte{0xd1, 0, 0}
			binary.BigEndian.PutUint16(b[1:], uint16(int16(i)))
			return b
		case i >= -2147483648:
			b := make([]byte, 5)
			b[0] = 0xd2
			binary.BigEndian.PutUint32(b[1:], uint32(int32(i)))
			return b
		default:
			b := make([]byte, 9)
			b[0] = 0xd3
			binary.BigEndian.PutUint64(b[1:], uint64(i))
			return b
		}
	}
	if float64(float32(f)) == f {
		b := make([]byte, 5)
		b[0] = 0xca
		binary.BigEndian.PutUint32(b[1:], math.Float32bits(float32(f)))
		return b
	}
	b := make([]byte, 9)
	b[0] = 0xcb
	binary.BigEndian.PutUint64(b[1:], math.Float64bits(f))
	return b
}

func cmsgpackEncodeString(s string) []byte {
	n := len(s)
	switch {
	case n <= 31:
		return append([]byte{0xa0 | byte(n)}, s...)
	case n <= 0xff:
		return append([]byte{0xd9, byte(n)}, s...)
	case n <= 0xffff:
		b := []byte{0xda, 0, 0}
		binary.BigEndian.PutUint16(b[1:], uint16(n))
		return append(b, s...)
	default:
		b := make([]byte, 5)
		b[0] = 0xdb
		binary.BigEndian.PutUint32(b[1:], uint32(n))
		return append(b, s...)
	}
}

// cmsgpackEncodeTable 全 1..n 连续整数键判数组（含空表→空数组），否则判 map
// （混合表按 map 编全部键值对，与真机一致）。
func cmsgpackEncodeTable(t *lua.LTable, level int) []byte {
	pairs := 0
	an := 0
	array := true
	max := 0
	t.ForEach(func(k, _ lua.LValue) {
		pairs++
		num, ok := k.(lua.LNumber)
		if !ok || math.Trunc(float64(num)) != float64(num) || int(num) < 1 {
			array = false
			return
		}
		an++
		if int(num) > max {
			max = int(num)
		}
	})
	if array && (an == 0 || max == an) {
		out := cmsgpackEncodeLen(0x90, 0xdc, 0xdd, an)
		for i := 1; i <= an; i++ {
			out = append(out, cmsgpackEncode(t.RawGetInt(i), level+1)...)
		}
		return out
	}
	out := cmsgpackEncodeLen(0x80, 0xde, 0xdf, pairs)
	t.ForEach(func(k, v lua.LValue) {
		out = append(out, cmsgpackEncode(k, level+1)...)
		out = append(out, cmsgpackEncode(v, level+1)...)
	})
	return out
}

func cmsgpackEncodeLen(fix, len16, len32 byte, n int) []byte {
	switch {
	case n <= 15:
		return []byte{fix | byte(n)}
	case n <= 0xffff:
		b := []byte{len16, 0, 0}
		binary.BigEndian.PutUint16(b[1:], uint16(n))
		return b
	default:
		b := make([]byte, 5)
		b[0] = len32
		binary.BigEndian.PutUint32(b[1:], uint32(n))
		return b
	}
}

type cmsgpackDecoder struct {
	s   string
	pos int
}

// value 解码一个值；截断报 Missing bytes，未知字节（含 0.4.0 不支持的
// bin/ext 类型）报 Bad data format。
func (d *cmsgpackDecoder) value(L *lua.LState) (lua.LValue, bool) {
	if d.pos >= len(d.s) {
		libErrorf(L, "Missing bytes in input.")
		return nil, false
	}
	b := d.s[d.pos]
	d.pos++
	switch {
	case b <= 0x7f:
		return lua.LNumber(b), true
	case b >= 0xe0:
		return lua.LNumber(int8(b)), true
	case b >= 0xa0 && b <= 0xbf:
		return d.bytes(L, int(b&0x1f))
	case b >= 0x90 && b <= 0x9f:
		return d.array(L, int(b&0x0f))
	case b >= 0x80 && b <= 0x8f:
		return d.mp(L, int(b&0x0f))
	case b == 0xc0:
		return lua.LNil, true
	case b == 0xc2:
		return lua.LBool(false), true
	case b == 0xc3:
		return lua.LBool(true), true
	case b == 0xca:
		f, ok := d.u32(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(math.Float32frombits(f)), true
	case b == 0xcb:
		f, ok := d.u64(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(math.Float64frombits(f)), true
	case b == 0xcc:
		v, ok := d.u8(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(v), true
	case b == 0xcd:
		v, ok := d.u16(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(v), true
	case b == 0xce:
		v, ok := d.u32(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(v), true
	case b == 0xcf:
		v, ok := d.u64(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(v), true
	case b == 0xd0:
		v, ok := d.u8(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(int8(v)), true
	case b == 0xd1:
		v, ok := d.u16(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(int16(v)), true
	case b == 0xd2:
		v, ok := d.u32(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(int32(v)), true
	case b == 0xd3:
		v, ok := d.u64(L)
		if !ok {
			return nil, false
		}
		return lua.LNumber(int64(v)), true
	case b == 0xd9:
		n, ok := d.u8(L)
		if !ok {
			return nil, false
		}
		return d.bytes(L, int(n))
	case b == 0xda:
		n, ok := d.u16(L)
		if !ok {
			return nil, false
		}
		return d.bytes(L, int(n))
	case b == 0xdb:
		n, ok := d.u32(L)
		if !ok {
			return nil, false
		}
		return d.bytes(L, int(n))
	case b == 0xdc:
		n, ok := d.u16(L)
		if !ok {
			return nil, false
		}
		return d.array(L, int(n))
	case b == 0xdd:
		n, ok := d.u32(L)
		if !ok {
			return nil, false
		}
		return d.array(L, int(n))
	case b == 0xde:
		n, ok := d.u16(L)
		if !ok {
			return nil, false
		}
		return d.mp(L, int(n))
	case b == 0xdf:
		n, ok := d.u32(L)
		if !ok {
			return nil, false
		}
		return d.mp(L, int(n))
	default:
		libErrorf(L, "Bad data format in input.")
		return nil, false
	}
}

func (d *cmsgpackDecoder) bytes(L *lua.LState, n int) (lua.LValue, bool) {
	if d.pos+n > len(d.s) {
		libErrorf(L, "Missing bytes in input.")
		return nil, false
	}
	s := d.s[d.pos : d.pos+n]
	d.pos += n
	return lua.LString(s), true
}

func (d *cmsgpackDecoder) array(L *lua.LState, n int) (lua.LValue, bool) {
	t := L.NewTable()
	for i := 1; i <= n; i++ {
		v, ok := d.value(L)
		if !ok {
			return nil, false
		}
		if v == nil {
			libErrorf(L, "Missing bytes in input.")
			return nil, false
		}
		t.RawSetInt(i, v)
	}
	return t, true
}

func (d *cmsgpackDecoder) mp(L *lua.LState, n int) (lua.LValue, bool) {
	t := L.NewTable()
	for i := 0; i < n; i++ {
		k, ok := d.value(L)
		if !ok {
			return nil, false
		}
		v, ok := d.value(L)
		if !ok {
			return nil, false
		}
		if k == nil || v == nil {
			libErrorf(L, "Missing bytes in input.")
			return nil, false
		}
		t.RawSet(k, v)
	}
	return t, true
}

func (d *cmsgpackDecoder) u8(L *lua.LState) (uint8, bool) {
	if d.pos+1 > len(d.s) {
		libErrorf(L, "Missing bytes in input.")
		return 0, false
	}
	v := d.s[d.pos]
	d.pos++
	return v, true
}

func (d *cmsgpackDecoder) u16(L *lua.LState) (uint16, bool) {
	if d.pos+2 > len(d.s) {
		libErrorf(L, "Missing bytes in input.")
		return 0, false
	}
	v := binary.BigEndian.Uint16([]byte(d.s[d.pos:]))
	d.pos += 2
	return v, true
}

func (d *cmsgpackDecoder) u32(L *lua.LState) (uint32, bool) {
	if d.pos+4 > len(d.s) {
		libErrorf(L, "Missing bytes in input.")
		return 0, false
	}
	v := binary.BigEndian.Uint32([]byte(d.s[d.pos:]))
	d.pos += 4
	return v, true
}

func (d *cmsgpackDecoder) u64(L *lua.LState) (uint64, bool) {
	if d.pos+8 > len(d.s) {
		libErrorf(L, "Missing bytes in input.")
		return 0, false
	}
	v := binary.BigEndian.Uint64([]byte(d.s[d.pos:]))
	d.pos += 8
	return v, true
}
