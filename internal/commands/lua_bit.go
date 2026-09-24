package commands

import (
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

// registerBitLib 注册 bit 全局表（对标 Redis 7.2 LuaBitOp：arshift/band/bnot/bor/
// bswap/bxor/lshift/rol/ror/rshift/tobit/tohex，32 位有符号语义）。
func registerBitLib(L *lua.LState) {
	lib := L.NewTable()
	L.SetFuncs(lib, map[string]lua.LGFunction{
		"arshift": bitShift("arshift"),
		"band":    bitFold("band"),
		"bor":     bitFold("bor"),
		"bxor":    bitFold("bxor"),
		"bnot":    bitUnary("bnot"),
		"bswap":   bitUnary("bswap"),
		"lshift":  bitShift("lshift"),
		"rol":     bitShift("rol"),
		"ror":     bitShift("ror"),
		"rshift":  bitShift("rshift"),
		"tobit":   bitTobit,
		"tohex":   bitTohex,
	})
	L.SetGlobal("bit", lib)
}

// bitToInt32 复刻 LuaBitOp 取参：number 四舍五入后折 32 位；string 经 tonumber
// 转换（失败按原类型报错）；余下类型直接报 number expected。
func bitToInt32(L *lua.LState, fname string, n int) (int32, bool) {
	v := L.Get(n)
	switch t := v.(type) {
	case lua.LNumber:
		return norm32(float64(t)), true
	case lua.LString:
		f, err := strconv.ParseFloat(strings.TrimSpace(string(t)), 64)
		if err != nil {
			bitRaise(L, fname, n, "string")
			return 0, false
		}
		return norm32(f), true
	case *lua.LNilType:
		if n > L.GetTop() {
			bitRaise(L, fname, n, "no value")
			return 0, false
		}
		bitRaise(L, fname, n, v.Type().String())
		return 0, false
	default:
		if n > L.GetTop() {
			bitRaise(L, fname, n, "no value")
			return 0, false
		}
		bitRaise(L, fname, n, v.Type().String())
		return 0, false
	}
}

func bitRaise(L *lua.LState, fname string, n int, got string) {
	libArgError(L, fname, n, "number expected, got "+got)
}

// norm32 四舍五入后折 32 位（探针：tobit(3.9)→4、tobit(4294967297)→1）。
func norm32(f float64) int32 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return int32(uint32(int64(math.Round(f))))
}

func bitTobit(L *lua.LState) int {
	v, ok := bitToInt32(L, "tobit", 1)
	if !ok {
		return 0
	}
	L.Push(lua.LNumber(v))
	return 1
}

// bitUnary 单参函数（bnot/bswap，多余参数忽略，与真机一致）。
func bitUnary(fname string) lua.LGFunction {
	return func(L *lua.LState) int {
		v, ok := bitToInt32(L, fname, 1)
		if !ok {
			return 0
		}
		switch fname {
		case "bnot":
			L.Push(lua.LNumber(^v))
		case "bswap":
			u := uint32(v)
			swapped := (u&0xFF)<<24 | (u&0xFF00)<<8 | (u&0xFF0000)>>8 | (u>>24)
			L.Push(lua.LNumber(int32(swapped)))
		}
		return 1
	}
}

// bitFold 变参折叠（band/bor/bxor，零参报 #1）。
func bitFold(fname string) lua.LGFunction {
	return func(L *lua.LState) int {
		top := L.GetTop()
		if top < 1 {
			bitRaise(L, fname, 1, "no value")
			return 0
		}
		acc, ok := bitToInt32(L, fname, 1)
		if !ok {
			return 0
		}
		for i := 2; i <= top; i++ {
			v, ok := bitToInt32(L, fname, i)
			if !ok {
				return 0
			}
			switch fname {
			case "band":
				acc &= v
			case "bor":
				acc |= v
			case "bxor":
				acc ^= v
			}
		}
		L.Push(lua.LNumber(acc))
		return 1
	}
}

// bitShift 双参位移/轮转（shift 量 &31，多余参数忽略）。
func bitShift(fname string) lua.LGFunction {
	return func(L *lua.LState) int {
		a, ok := bitToInt32(L, fname, 1)
		if !ok {
			return 0
		}
		s, ok := bitToInt32(L, fname, 2)
		if !ok {
			return 0
		}
		n := uint(uint32(s) & 31)
		ua := uint32(a)
		var out int32
		switch fname {
		case "lshift":
			out = int32(ua << n)
		case "rshift":
			out = int32(ua >> n)
		case "arshift":
			out = a >> n
		case "rol":
			out = int32(bits.RotateLeft32(ua, int(n)))
		case "ror":
			out = int32(bits.RotateLeft32(ua, -int(n)))
		}
		L.Push(lua.LNumber(out))
		return 1
	}
}

// bitTohex 格式化无符号 32 位 hex（默认 8 位小写；n 为负时大写，宽度取绝对值）。
func bitTohex(L *lua.LState) int {
	v, ok := bitToInt32(L, "tohex", 1)
	if !ok {
		return 0
	}
	width := 8
	upper := false
	if L.GetTop() >= 2 {
		n, ok := bitToInt32(L, "tohex", 2)
		if !ok {
			return 0
		}
		if n < 0 {
			upper = true
			width = int(-n)
		} else {
			width = int(n)
		}
	}
	u := uint32(v)
	var full string
	if upper {
		full = fmt.Sprintf("%08X", u)
	} else {
		full = fmt.Sprintf("%08x", u)
	}
	var out string
	switch {
	case width == 8:
		out = full
	case width < 8:
		if width <= 0 {
			out = ""
		} else {
			out = full[8-width:]
		}
	default:
		out = strings.Repeat("0", width-8) + full
		// 大写分支 full 已是大写，前导 0 不影响。
	}
	L.Push(lua.LString(out))
	return 1
}
