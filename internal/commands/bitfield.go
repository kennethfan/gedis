package commands

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *bitmapHandler) registerField(r *network.Router) {
	r.Register("BITFIELD", h.bitfield)
}

type bitfieldType struct {
	signed bool
	bits   int
}

type bitOverflow int

const (
	bitOverflowWrap bitOverflow = iota
	bitOverflowSat
	bitOverflowFail
)

const bitfieldTypeErr = "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."

func parseBitfieldType(s string) (bitfieldType, error) {
	if len(s) < 2 {
		return bitfieldType{}, fmt.Errorf("%s", bitfieldTypeErr)
	}
	var signed bool
	switch s[0] {
	case 'u', 'U':
	case 'i', 'I':
		signed = true
	default:
		return bitfieldType{}, fmt.Errorf("%s", bitfieldTypeErr)
	}
	n, err := strconv.Atoi(s[1:])
	if err != nil || s[1:] == "" {
		return bitfieldType{}, fmt.Errorf("%s", bitfieldTypeErr)
	}
	if !signed && (n < 1 || n > 63) {
		return bitfieldType{}, fmt.Errorf("%s", bitfieldTypeErr)
	}
	if signed && (n < 1 || n > 64) {
		return bitfieldType{}, fmt.Errorf("%s", bitfieldTypeErr)
	}
	return bitfieldType{signed: signed, bits: n}, nil
}

const bitOffsetErr = "ERR bit offset is not an integer or out of range"

// parseBitfieldOffset 解析位移：整数或 #N（N 倍 width）；范围 [0, 2^32)。
func parseBitfieldOffset(s string, width int) (uint64, error) {
	if strings.HasPrefix(s, "#") {
		n, err := strconv.ParseUint(s[1:], 10, 32)
		if err != nil || s[1:] == "" {
			return 0, fmt.Errorf("%s", bitOffsetErr)
		}
		return n * uint64(width), nil
	}
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s", bitOffsetErr)
	}
	return n, nil
}

func bitfieldMask(bits int) uint64 {
	if bits == 64 {
		return math.MaxUint64
	}
	return uint64(1)<<uint(bits) - 1
}

func bitfieldBounds(t bitfieldType) (int64, int64) {
	if !t.signed {
		return 0, int64(bitfieldMask(t.bits))
	}
	if t.bits == 64 {
		return math.MinInt64, math.MaxInt64
	}
	max := int64(uint64(1)<<uint(t.bits-1)) - 1
	return -max - 1, max
}

// bitfieldRaw 按大端 bit 序读 width 位为 uint64；越界补零。
func bitfieldRaw(buf []byte, off uint64, bits int) uint64 {
	var v uint64
	for i := 0; i < bits; i++ {
		v <<= 1
		idx := off + uint64(i)
		if idx/8 < uint64(len(buf)) && buf[idx/8]&(0x80>>(idx%8)) != 0 {
			v |= 1
		}
	}
	return v
}

func bitfieldSignExtend(v uint64, t bitfieldType) int64 {
	if t.signed && t.bits < 64 && v>>(uint(t.bits)-1)&1 == 1 {
		return int64(v) - int64(uint64(1)<<uint(t.bits))
	}
	return int64(v)
}

func bitfieldGet(buf []byte, off uint64, t bitfieldType) int64 {
	return bitfieldSignExtend(bitfieldRaw(buf, off, t.bits), t)
}

// bitfieldSet 写 width 位（值恒截断），返回旧值与新 buf。
func bitfieldSet(buf []byte, off uint64, t bitfieldType, val int64) (int64, []byte) {
	old := bitfieldGet(buf, off, t)
	v := uint64(val) & bitfieldMask(t.bits)
	need := (off + uint64(t.bits) + 7) / 8
	for uint64(len(buf)) < need {
		buf = append(buf, 0)
	}
	for i := 0; i < t.bits; i++ {
		idx := off + uint64(i)
		if v>>(uint(t.bits)-1-uint(i))&1 == 1 {
			buf[idx/8] |= 0x80 >> (idx % 8)
		} else {
			buf[idx/8] &^= 0x80 >> (idx % 8)
		}
	}
	return old, buf
}

// bitfieldIncrby 按溢出模式累加；FAIL 溢出时 overflowed=true 且不写回。
func bitfieldIncrby(buf []byte, off uint64, t bitfieldType, incr int64, ov bitOverflow) (int64, []byte, bool) {
	old := bitfieldGet(buf, off, t)
	min, max := bitfieldBounds(t)
	overflowed := false
	if incr >= 0 {
		overflowed = old > max-incr
	} else {
		overflowed = old < min-incr
	}
	switch ov {
	case bitOverflowFail:
		if overflowed {
			return 0, buf, true
		}
	case bitOverflowSat:
		if overflowed {
			if incr >= 0 {
				_, out := bitfieldSet(buf, off, t, max)
				return max, out, false
			}
			_, out := bitfieldSet(buf, off, t, min)
			return min, out, false
		}
	}
	raw := uint64(old) + uint64(incr)
	res := bitfieldSignExtend(raw&bitfieldMask(t.bits), t)
	_, out := bitfieldSet(buf, off, t, res)
	return res, out, false
}

func (h *bitmapHandler) bitfield(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'bitfield' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	buf, expiry, err := h.readBytes(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		buf = nil
	}
	overflow := bitOverflowWrap
	dirty := false
	out := make([]protocol.Value, 0, 4)
	i := 1
	for ; i < len(args); i++ {
		name, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(name) {
		case "GET":
			if i+2 >= len(args) {
				return errValueStr("ERR syntax error")
			}
			typ, off, errReply := parseFieldTypeOffset(args[i+1], args[i+2])
			if errReply != nil {
				return *errReply
			}
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: bitfieldGet(buf, off, typ)})
			i += 2
		case "SET":
			if i+3 >= len(args) {
				return errValueStr("ERR syntax error")
			}
			typ, off, errReply := parseFieldTypeOffset(args[i+1], args[i+2])
			if errReply != nil {
				return *errReply
			}
			valStr, ok := argString(args[i+3])
			if !ok {
				return errValueStr("ERR value is not an integer or out of range")
			}
			val, err := strconv.ParseInt(valStr, 10, 64)
			if err != nil {
				return errValueStr("ERR value is not an integer or out of range")
			}
			old, next := bitfieldSet(buf, off, typ, val)
			buf = next
			dirty = true
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: old})
			i += 3
		case "INCRBY":
			if i+3 >= len(args) {
				return errValueStr("ERR syntax error")
			}
			typ, off, errReply := parseFieldTypeOffset(args[i+1], args[i+2])
			if errReply != nil {
				return *errReply
			}
			incrStr, ok := argString(args[i+3])
			if !ok {
				return errValueStr("ERR value is not an integer or out of range")
			}
			incr, err := strconv.ParseInt(incrStr, 10, 64)
			if err != nil {
				return errValueStr("ERR value is not an integer or out of range")
			}
			res, next, overflowed := bitfieldIncrby(buf, off, typ, incr, overflow)
			if overflowed {
				out = append(out, protocol.Value{Kind: protocol.KindBulkString})
				i += 3
				continue
			}
			buf = next
			dirty = true
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: res})
			i += 3
		case "OVERFLOW":
			if i+1 >= len(args) {
				return errValueStr("ERR syntax error")
			}
			mode, ok := argString(args[i+1])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			switch strings.ToUpper(mode) {
			case "WRAP":
				overflow = bitOverflowWrap
			case "SAT":
				overflow = bitOverflowSat
			case "FAIL":
				overflow = bitOverflowFail
			default:
				return errValueStr("ERR syntax error")
			}
			i++
		default:
			return errValueStr("ERR syntax error")
		}
	}
	if dirty {
		if werr := h.writeBytes(ctx, key, buf, expiry); werr != nil {
			return errValue(werr)
		}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func parseFieldTypeOffset(tArg, oArg protocol.Value) (bitfieldType, uint64, *protocol.Value) {
	typeStr, ok := argString(tArg)
	if !ok {
		v := errValueStr(bitfieldTypeErr)
		return bitfieldType{}, 0, &v
	}
	typ, err := parseBitfieldType(typeStr)
	if err != nil {
		v := errValue(err)
		return bitfieldType{}, 0, &v
	}
	offStr, ok := argString(oArg)
	if !ok {
		v := errValueStr(bitOffsetErr)
		return bitfieldType{}, 0, &v
	}
	off, err := parseBitfieldOffset(offStr, typ.bits)
	if err != nil {
		v := errValue(err)
		return bitfieldType{}, 0, &v
	}
	return typ, off, nil
}
