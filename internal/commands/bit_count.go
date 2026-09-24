package commands

import (
	"context"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *bitmapHandler) registerCount(r *network.Router) {
	r.Register("BITCOUNT", h.bitcount)
	r.Register("BITPOS", h.bitpos)
}

// popCountBits 统计 buf 二进制中 1 的个数。
func popCountBits(buf []byte) int64 {
	var n int64
	for _, b := range buf {
		for b != 0 {
			n += int64(b & 1)
			b >>= 1
		}
	}
	return n
}

// bitRangeToBytes 把 [start, end]（BYTE 或 BIT 单位，支持负数）换算为闭合 bit 区间。
// 返回 (fromBit, toBit, ok)；空区间 ok=false。
func bitRangeToBytes(totalBits int64, start, end int64, byBit bool) (int64, int64, bool) {
	if !byBit {
		totalBytes := totalBits / 8
		if start < 0 {
			start += totalBytes
		}
		if end < 0 {
			end += totalBytes
		}
		if start < 0 {
			start = 0
		}
		if end >= totalBytes {
			end = totalBytes - 1
		}
		if start > end {
			return 0, -1, false
		}
		return start * 8, end*8 + 7, true
	}
	if start < 0 {
		start += totalBits
	}
	if end < 0 {
		end += totalBits
	}
	if start < 0 {
		start = 0
	}
	if end >= totalBits {
		end = totalBits - 1
	}
	if start > end {
		return 0, -1, false
	}
	return start, end, true
}

func parseBitUnit(s string) (byBit bool, ok bool) {
	switch strings.ToUpper(s) {
	case "BYTE":
		return false, true
	case "BIT":
		return true, true
	}
	return false, false
}

func (h *bitmapHandler) bitcount(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 && len(args) != 3 && len(args) != 4 {
		return errValueStr("ERR wrong number of arguments for 'bitcount' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	var start, end int64
	byBit := false
	if len(args) >= 3 {
		s, ok1 := argString(args[1])
		e, ok2 := argString(args[2])
		if !ok1 || !ok2 {
			return errValueStr("ERR value is not an integer or out of range")
		}
		var err error
		start, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
		end, err = strconv.ParseInt(e, 10, 64)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
		if len(args) == 4 {
			u, ok := argString(args[3])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			byBit, ok = parseBitUnit(u)
			if !ok {
				return errValueStr("ERR syntax error")
			}
		}
	}
	buf, _, err := h.readBytes(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	if len(args) == 1 {
		return protocol.Value{Kind: protocol.KindInteger, I: popCountBits(buf)}
	}
	from, to, ok := bitRangeToBytes(int64(len(buf))*8, start, end, byBit)
	if !ok {
		return protocol.Value{Kind: protocol.KindInteger}
	}
	var n int64
	for b := from; b <= to; b++ {
		n += getBitAt(buf, uint64(b))
	}
	return protocol.Value{Kind: protocol.KindInteger, I: n}
}

func (h *bitmapHandler) bitpos(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 || len(args) > 5 {
		return errValueStr("ERR wrong number of arguments for 'bitpos' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	bitStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR The bit argument must be 1 or 0.")
	}
	if bitStr != "0" && bitStr != "1" {
		return errValueStr("ERR The bit argument must be 1 or 0.")
	}
	want := int64(0)
	if bitStr == "1" {
		want = 1
	}
	var start, end int64
	endGiven := false
	byBit := false
	if len(args) >= 3 {
		s, ok := argString(args[2])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		var err error
		start, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
	}
	if len(args) >= 4 {
		e, ok := argString(args[3])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		var err error
		end, err = strconv.ParseInt(e, 10, 64)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
		endGiven = true
	}
	if len(args) == 5 {
		u, ok := argString(args[4])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		byBit, ok = parseBitUnit(u)
		if !ok {
			return errValueStr("ERR syntax error")
		}
	}
	buf, _, err := h.readBytes(ctx, key)
	if err != nil {
		if isNotFound(err) {
			if want == 1 {
				return protocol.Value{Kind: protocol.KindInteger, I: -1}
			}
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	totalBits := int64(len(buf)) * 8
	if !byBit {
		totalUnits := totalBits / 8
		if start < 0 {
			start += totalUnits
		}
		if endGiven && end < 0 {
			end += totalUnits
		}
		if start < 0 {
			start = 0
		}
		from := start * 8
		var to int64
		if !endGiven {
			to = -1
		} else {
			if end < 0 {
				return protocol.Value{Kind: protocol.KindInteger, I: -1}
			}
			to = end*8 + 7
			if to >= totalBits {
				to = totalBits - 1
			}
		}
		return protocol.Value{Kind: protocol.KindInteger, I: findBit(buf, want, from, to, totalBits)}
	}
	if start < 0 {
		start += totalBits
	}
	if endGiven && end < 0 {
		end += totalBits
	}
	if start < 0 {
		start = 0
	}
	to := int64(-1)
	if endGiven {
		if end < 0 {
			return protocol.Value{Kind: protocol.KindInteger, I: -1}
		}
		to = end
		if to >= totalBits {
			to = totalBits - 1
		}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: findBit(buf, want, start, to, totalBits)}
}

// findBit 在 [from, to]（to<0 表无穷）找首个 want 位；无则 -1。
// 无 end 时 bit0 可落到填充零区（首个零位可能 >= totalBits）。
func findBit(buf []byte, want, from, to, totalBits int64) int64 {
	if to < 0 {
		if want == 1 {
			to = totalBits - 1
		} else {
			if from >= totalBits {
				return -1
			}
			for b := from; b < totalBits; b++ {
				if getBitAt(buf, uint64(b)) == 0 {
					return b
				}
			}
			return totalBits
		}
	}
	if from > to {
		return -1
	}
	for b := from; b <= to; b++ {
		var v int64
		if b < totalBits {
			v = getBitAt(buf, uint64(b))
		}
		if v == want {
			return b
		}
	}
	return -1
}
