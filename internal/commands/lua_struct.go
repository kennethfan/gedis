package commands

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"

	lua "github.com/yuin/gopher-lua"
)

// registerStructLib 注册 struct 全局表（对标 Redis 7.2 内嵌 struct 库：
// pack/unpack/size，> 大端 / < 小端 / !n 对齐，i/I/c 变长）。
func registerStructLib(L *lua.LState) {
	lib := L.NewTable()
	L.SetFuncs(lib, map[string]lua.LGFunction{
		"pack":   structPack,
		"unpack": structUnpack,
		"size":   structSize,
	})
	L.SetGlobal("struct", lib)
}

// structNativeLittle 报告宿主字节序（无前缀格式的默认端序，真机与 gedis 同为小端主流架构）。
func structNativeLittle() bool {
	x := uint16(1)
	return *(*byte)(unsafe.Pointer(&x)) == 1
}

// structItem 为格式串中的一个条目；maxalign 为其生效的 ! 对齐上限（-1 表示无对齐）。
type structItem struct {
	code     byte
	size     int // 数字/c 的字节数；c0 为 0 占位；x 为 1；s 不用
	c0       bool
	maxalign int  // 生效的 ! 对齐上限（-1 表示无对齐）
	little   bool // 该条目生效的端序（> / < 可在格式中途切换）
}

// structAlign 返回条目的自然对齐（数字按字节数，c/s/x 按 1；0 长按 1 防除零）。
func structAlign(it structItem) int {
	if it.code == 'c' || it.code == 's' || it.code == 'x' {
		return 1
	}
	if it.size <= 0 {
		return 1
	}
	return it.size
}

// structPad 按 min(自然对齐, maxalign) 把偏移量向上对齐（无对齐时原样返回）。
func structPad(off, align, maxalign int) int {
	if maxalign < 0 || align <= 1 {
		return off
	}
	a := align
	if a > maxalign {
		a = maxalign
	}
	if r := off % a; r != 0 {
		off += a - r
	}
	return off
}

// structGot 报告第 n 个参数的类型名（缺参透出 nil 的 "nil"，调用方按需区分 "no value"）。
func structGot(L *lua.LState, n int) string {
	return L.Get(n).Type().String()
}

// structNumString 复刻 Lua 5.1 tostring(number)：整数按十进制，小数按 %.14g。
func structNumString(f float64) string {
	if math.Trunc(f) == f && math.Abs(f) < 1<<53 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', 14, 64)
}

// structFmtArg 取格式串参数：string 直接用，number 按 tostring 折，其余报 string expected。
// 缺参（n > top）报 "no value"，显式 nil 报 "nil"。
func structFmtArg(L *lua.LState, fname string, n int) (string, bool) {
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
		libArgError(L, fname, n, "string expected, got "+structGot(L, n))
		return "", false
	}
}

// structParse 解析格式串；strict 下非法选项报 invalid format option，
// size 模式（strict=false）下静默跳过。空格跳过，> / < 切换端序，!n 设对齐。
// 返回条目与最终端序（unpack/pack 共用，避免二次重放）。
func structParse(L *lua.LState, fname, f string, strict bool) ([]structItem, bool, bool) {
	var items []structItem
	little := structNativeLittle()
	maxalign := -1
	invalid := func(ch byte) ([]structItem, bool, bool) {
		libArgError(L, fname, 1, fmt.Sprintf("invalid format option '%c'", ch))
		return nil, false, false
	}
	i := 0
	for i < len(f) {
		ch := f[i]
		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\v' || ch == '\f':
			i++
		case ch == '>' || ch == '<':
			little = ch == '<'
			i++
		case ch == '!':
			j := i + 1
			for j < len(f) && f[j] >= '0' && f[j] <= '9' {
				j++
			}
			n := 8 // 裸 ! 取原生最大对齐（探针 '!c1I16' → 24 锁定为 8）
			if j > i+1 {
				n, _ = strconv.Atoi(f[i+1 : j])
			}
		if n == 0 || n&(n-1) != 0 {
			libErrorf(L, "alignment %d is not a power of 2", n)
			return nil, false, false
		}
			maxalign = n
			i = j
		case ch >= '0' && ch <= '9':
			if strict {
				return invalid(ch)
			}
			i++
		default:
			code := ch
			switch code {
			case 'b', 'B', 'h', 'H', 'i', 'I', 'l', 'L', 'T', 'f', 'd', 'c', 's', 'x':
				i++
				size := 0
				switch code {
				case 'b', 'B':
					size = 1
				case 'h', 'H':
					size = 2
				case 'i', 'I':
					size = 4
				case 'l', 'L', 'T':
					size = 8
				case 'f':
					size = 4
				case 'd':
					size = 8
				case 'c':
					size = 1
				case 'x':
					size = 1
				}
				if code == 'i' || code == 'I' || code == 'c' {
					j := i
					for j < len(f) && f[j] >= '0' && f[j] <= '9' {
						j++
					}
					if j > i {
						size, _ = strconv.Atoi(f[i:j])
						i = j
					}
				} else if i < len(f) && f[i] >= '0' && f[i] <= '9' {
					// b/h/l/f/d/s/x 不接受长度后缀（探针 '>h2' → invalid '2'）
					if strict {
						return invalid(f[i])
					}
				}
				items = append(items, structItem{code: code, size: size, c0: code == 'c' && size == 0, maxalign: maxalign, little: little})
			default:
				if strict {
					return invalid(code)
				}
				i++
			}
		}
	}
	return items, little, true
}

// structPack 按格式编码入参（多余参数忽略；x 不耗参）。
func structPack(L *lua.LState) int {
	f, ok := structFmtArg(L, "pack", 1)
	if !ok {
		return 0
	}
	items, _, ok := structParse(L, "pack", f, true)
	if !ok {
		return 0
	}
	var out []byte
	off := 0
	arg := 2
	for _, it := range items {
		off = structPad(off, structAlign(it), it.maxalign)
		for len(out) < off {
			out = append(out, 0)
		}
		switch it.code {
		case 'x':
			out = append(out, 0)
			off++
		case 's':
			s, ok := structStrArg(L, "pack", arg)
			if !ok {
				return 0
			}
			arg++
			out = append(out, s...)
			out = append(out, 0)
			off += len(s) + 1
		case 'c':
			s, ok := structStrArg(L, "pack", arg)
			if !ok {
				return 0
			}
		if len(s) < it.size && !it.c0 {
			// 真机 off-by-one：短串报错位为值参数位 +1
			libArgError(L, "pack", arg+1, "string too short")
			return 0
		}
			arg++
			if it.c0 {
				out = append(out, s...)
				off += len(s)
			} else {
				out = append(out, s[:it.size]...)
				off += it.size
			}
		default:
			v, ok := structNumArg(L, "pack", arg)
			if !ok {
				return 0
			}
			arg++
			out = structPackNum(out, it, v, it.little)
			off += it.size
		}
	}
	L.Push(lua.LString(string(out)))
	return 1
}

// structStrArg 取 pack 的串参数：string 直接用，number 按 tostring 折；
// 缺参与显式 nil 均报类型名（"nil"），其余类型同理。
func structStrArg(L *lua.LState, fname string, n int) (string, bool) {
	switch t := L.Get(n).(type) {
	case lua.LString:
		return string(t), true
	case lua.LNumber:
		return structNumString(float64(t)), true
	default:
		libArgError(L, fname, n, "string expected, got "+structGot(L, n))
		return "", false
	}
}

// structNumArg 取 pack 的数值参数：仅 number（向零截断由调用方处理）；
// 缺参报 "nil"，其余类型报类型名。
func structNumArg(L *lua.LState, fname string, n int) (float64, bool) {
	if t, ok := L.Get(n).(lua.LNumber); ok {
		return float64(t), true
	}
	libArgError(L, fname, n, "number expected, got "+structGot(L, n))
	return 0, false
}

// structPackNum 编码单个数字条目（整数向零截断后按宽度回绕，f/d 按 IEEE754）。
func structPackNum(out []byte, it structItem, v float64, little bool) []byte {
	switch it.code {
	case 'b':
		return append(out, byte(int8(int64(math.Trunc(v)))))
	case 'B':
		return append(out, byte(uint8(int64(math.Trunc(v)))))
	case 'h', 'H', 'i', 'I', 'l', 'L', 'T':
		signed := it.code == 'b' || it.code == 'h' || it.code == 'i' || it.code == 'l'
		return structPackInt(out, math.Trunc(v), it.size, little, signed)
	case 'f':
		var b [4]byte
		if little {
			binary.LittleEndian.PutUint32(b[:], math.Float32bits(float32(v)))
		} else {
			binary.BigEndian.PutUint32(b[:], math.Float32bits(float32(v)))
		}
		return append(out, b[:]...)
	default: // 'd'
		var b [8]byte
		if little {
			binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
		} else {
			binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
		}
		return append(out, b[:]...)
	}
}

// structPackInt 按宽度回绕写入整数（>8 字节时高位按符号填充，与 float64 精度语义一致）。
func structPackInt(out []byte, v float64, n int, little, signed bool) []byte {
	if n <= 0 {
		return out
	}
	u := uint64(int64(v))
	if n <= 8 {
		for i := 0; i < n; i++ {
			if little {
				out = append(out, byte(u>>(8*i)))
			} else {
				out = append(out, byte(u>>(8*(n-1-i))))
			}
		}
		return out
	}
	fill := byte(0)
	if signed && int64(v) < 0 {
		fill = 0xFF
	}
	if little {
		for i := 0; i < 8; i++ {
			out = append(out, byte(u>>(8*i)))
		}
		for i := 8; i < n; i++ {
			out = append(out, fill)
		}
		return out
	}
	for i := 8; i < n; i++ {
		out = append(out, fill)
	}
	for i := 0; i < 8; i++ {
		out = append(out, byte(u>>(8*(7-i))))
	}
	return out
}

// structUnpack 按格式解码（返回 值... + 下个位置 1-based；尾随字节忽略）。
func structUnpack(L *lua.LState) int {
	f, ok := structFmtArg(L, "unpack", 1)
	if !ok {
		return 0
	}
	items, _, ok := structParse(L, "unpack", f, true)
	if !ok {
		return 0
	}
	data, ok := structDataArg(L, "unpack", 2)
	if !ok {
		return 0
	}
	off, ok := structOffsetArg(L, "unpack", 3)
	if !ok {
		return 0
	}
	if off == 0 {
		libArgError(L, "unpack", 3, "offset must be 1 or greater")
		return 0
	}
	pos := off - 1
	if pos < 0 || pos > len(data) {
		libArgError(L, "unpack", 2, "data string too short")
		return 0
	}
	var rets []lua.LValue
	var prev float64
	prevValid := false
	for _, it := range items {
		pos = structPad(pos, structAlign(it), it.maxalign)
		switch it.code {
		case 'x':
			if pos+1 > len(data) {
				return retShort(L)
			}
			pos++
			prevValid = false
		case 's':
			idx := strings.IndexByte(data[pos:], 0)
			if idx < 0 {
				libErrorf(L, "unfinished string in data")
				return 0
			}
			rets = append(rets, lua.LString(data[pos:pos+idx]))
			pos += idx + 1
			prevValid = false
		case 'c':
			n := it.size
			if it.c0 {
				if !prevValid {
					libErrorf(L, "format 'c0' needs a previous size")
					return 0
				}
				n = int(prev)
				// c0 吞掉前一个数值（探针 '>BBc0' → (2,'abc',6)：只吞紧邻的上一个）
				rets = rets[:len(rets)-1]
			}
			if n < 0 || pos+n > len(data) {
				return retShort(L)
			}
			rets = append(rets, lua.LString(data[pos:pos+n]))
			pos += n
			prevValid = false
		default:
			if pos+it.size > len(data) {
				return retShort(L)
			}
			v := structUnpackNum([]byte(data[pos:pos+it.size]), it, it.little)
			rets = append(rets, lua.LNumber(v))
			pos += it.size
			prev = v
			prevValid = true
		}
	}
	for _, v := range rets {
		L.Push(v)
	}
	L.Push(lua.LNumber(pos + 1))
	return len(rets) + 1
}

func retShort(L *lua.LState) int {
	libArgError(L, "unpack", 2, "data string too short")
	return 0
}

// structDataArg 取 unpack 数据串：缺参报 "no value"，number 按 tostring 折。
func structDataArg(L *lua.LState, fname string, n int) (string, bool) {
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
		libArgError(L, fname, n, "string expected, got "+structGot(L, n))
		return "", false
	}
}

// structOffsetArg 取 unpack 起始偏移（1-based；缺参/显式 nil 默认为 1，向零截断）。
func structOffsetArg(L *lua.LState, fname string, n int) (int, bool) {
	if n > L.GetTop() {
		return 1, true
	}
	v := L.Get(n)
	if _, isNil := v.(*lua.LNilType); isNil {
		return 1, true
	}
	if t, ok := v.(lua.LNumber); ok {
		return int(float64(t)), true
	}
	libArgError(L, fname, n, "number expected, got "+structGot(L, n))
	return 0, false
}

// structUnpackNum 解码单个数字条目（整数按符号扩展后经 float64 透出，f/d 按 IEEE754）。
func structUnpackNum(b []byte, it structItem, little bool) float64 {
	n := len(b)
	switch it.code {
	case 'b':
		return float64(int8(b[0]))
	case 'B':
		return float64(b[0])
	case 'h', 'H':
		var u uint16
		if little {
			u = binary.LittleEndian.Uint16(b)
		} else {
			u = binary.BigEndian.Uint16(b)
		}
		if it.code == 'h' {
			return float64(int16(u))
		}
		return float64(u)
	case 'f':
		var u uint32
		if little {
			u = binary.LittleEndian.Uint32(b)
		} else {
			u = binary.BigEndian.Uint32(b)
		}
		return float64(math.Float32frombits(u))
	case 'd':
		var u uint64
		if little {
			u = binary.LittleEndian.Uint64(b)
		} else {
			u = binary.BigEndian.Uint64(b)
		}
		return math.Float64frombits(u)
	default: // i/I/l/L/T
		signed := it.code == 'i' || it.code == 'l'
		if n == 0 {
			return 0
		}
		if n <= 8 {
			var u uint64
			if little {
				for i := 0; i < n; i++ {
					u |= uint64(b[i]) << (8 * i)
				}
			} else {
				for i := 0; i < n; i++ {
					u = u<<8 | uint64(b[i])
				}
			}
			if signed {
				shift := 64 - 8*n
				return float64(int64(u<<uint(shift)) >> uint(shift))
			}
			return float64(u)
		}
		// 超 8 字节：按 double 累加后补符号（小值精确，大值随 double 舍入）。
		var v float64
		if little {
			for i := n - 1; i >= 0; i-- {
				v = v*256 + float64(b[i])
			}
			if signed && b[n-1]&0x80 != 0 {
				v -= math.Ldexp(1, 8*n)
			}
		} else {
			for i := 0; i < n; i++ {
				v = v*256 + float64(b[i])
			}
			if signed && b[0]&0x80 != 0 {
				v -= math.Ldexp(1, 8*n)
			}
		}
		return v
	}
}

// structSize 计算格式的定长字节数（非法选项静默跳过；s/c0 报无固定长度）。
func structSize(L *lua.LState) int {
	f, ok := structFmtArg(L, "size", 1)
	if !ok {
		return 0
	}
	items, _, ok := structParse(L, "size", f, false)
	if !ok {
		return 0
	}
	off := 0
	for _, it := range items {
		if it.code == 's' {
			libArgError(L, "size", 1, "option 's' has no fixed size")
			return 0
		}
		if it.c0 {
			libArgError(L, "size", 1, "option 'c0' has no fixed size")
			return 0
		}
		off = structPad(off, structAlign(it), it.maxalign)
		off += it.size
	}
	L.Push(lua.LNumber(off))
	return 1
}
