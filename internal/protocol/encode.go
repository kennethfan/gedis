package protocol

import (
	"math"
	"strconv"
)

// Append 把 v 编码为标准 RESP 字节追加到 dst。Bulk nil 表 null bulk，
// Array Elems nil 表 null array（非 nil 空数组编码为 *0）。
func (v Value) Append(dst []byte) []byte {
	switch v.Kind {
	case KindSimpleString:
		return appendLine(dst, '+', v.S)
	case KindError:
		return appendLine(dst, '-', v.S)
	case KindInteger:
		return appendLine(dst, ':', strconv.FormatInt(v.I, 10))
	case KindBulkString:
		return appendBulk(dst, '$', v.Bulk)
	case KindArray:
		return appendElems(dst, '*', v.Elems)
	case KindNull:
		return append(dst, "_\r\n"...)
	case KindBoolean:
		if v.B {
			return append(dst, "#t\r\n"...)
		}
		return append(dst, "#f\r\n"...)
	case KindDouble:
		return appendLine(dst, ',', formatDouble(v.F))
	case KindBigNumber:
		return appendLine(dst, '(', v.S)
	case KindBulkError:
		return appendBulk(dst, '!', v.Bulk)
	case KindVerbatim:
		body := v.VerbatimFmt + ":" + string(v.Bulk)
		dst = append(dst, '=')
		dst = strconv.AppendInt(dst, int64(len(body)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, body...)
		return append(dst, '\r', '\n')
	case KindMap:
		return appendPairs(dst, '%', v.Pairs)
	case KindSet:
		return appendElems(dst, '~', v.Elems)
	case KindAttribute:
		return appendPairs(dst, '|', v.Pairs)
	case KindPush:
		return appendElems(dst, '>', v.Elems)
	default:
		return appendLine(dst, '-', "ERR unknown kind")
	}
}

func appendLine(dst []byte, typ byte, s string) []byte {
	dst = append(dst, typ)
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

func appendBulk(dst []byte, typ byte, b []byte) []byte {
	if b == nil {
		return append(dst, typ, '-', '1', '\r', '\n')
	}
	dst = append(dst, typ)
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, b...)
	return append(dst, '\r', '\n')
}

func appendElems(dst []byte, typ byte, elems []Value) []byte {
	if elems == nil {
		return append(dst, typ, '-', '1', '\r', '\n')
	}
	dst = append(dst, typ)
	dst = strconv.AppendInt(dst, int64(len(elems)), 10)
	dst = append(dst, '\r', '\n')
	for _, e := range elems {
		dst = e.Append(dst)
	}
	return dst
}

func appendPairs(dst []byte, typ byte, pairs []Pair) []byte {
	dst = append(dst, typ)
	dst = strconv.AppendInt(dst, int64(len(pairs)), 10)
	dst = append(dst, '\r', '\n')
	for _, p := range pairs {
		dst = p.K.Append(dst)
		dst = p.V.Append(dst)
	}
	return dst
}

func formatDouble(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	default:
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
}
