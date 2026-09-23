package commands

import (
	"context"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *bitmapHandler) registerOp(r *network.Router) {
	r.Register("BITOP", h.bitop)
}

// bitop 实现 BITOP op dest srckeys...：结果长度取最长输入，缺失源按零填充；
// 空结果删除 dest（含已存在的 dest）；NOT 仅允许单源。
func (h *bitmapHandler) bitop(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'bitop' command")
	}
	opStr, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	dst, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	op := strings.ToUpper(opStr)
	if op != "AND" && op != "OR" && op != "XOR" && op != "NOT" {
		return errValueStr("ERR syntax error")
	}
	if op == "NOT" && len(args) != 3 {
		return errValueStr("ERR BITOP NOT must be called with a single source key.")
	}
	srcs := make([][]byte, 0, len(args)-2)
	for _, a := range args[2:] {
		name, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid key")
		}
		buf, _, err := h.readBytes(ctx, name)
		if err != nil {
			if isNotFound(err) {
				srcs = append(srcs, nil)
				continue
			}
			return errValue(err)
		}
		srcs = append(srcs, buf)
	}
	maxLen := 0
	for _, s := range srcs {
		if len(s) > maxLen {
			maxLen = len(s)
		}
	}
	if maxLen == 0 {
		if draw, de, err := lookupRaw(ctx, h.kv, dst); err == nil {
			h.deleteBitmap(ctx, draw, de, dst)
		} else if !isNotFound(err) {
			return errValue(err)
		}
		return protocol.Value{Kind: protocol.KindInteger}
	}
	out := make([]byte, maxLen)
	switch op {
	case "AND":
		for i := range out {
			out[i] = 0xFF
		}
		for _, s := range srcs {
			for i := range out {
				var b byte
				if i < len(s) {
					b = s[i]
				}
				out[i] &= b
			}
		}
	case "OR":
		for _, s := range srcs {
			for i := range out {
				if i < len(s) {
					out[i] |= s[i]
				}
			}
		}
	case "XOR":
		for _, s := range srcs {
			for i := range out {
				if i < len(s) {
					out[i] ^= s[i]
				}
			}
		}
	case "NOT":
		for i := range out {
			out[i] = ^srcs[0][i]
		}
	}
	if draw, de, err := lookupRaw(ctx, h.kv, dst); err == nil {
		h.deleteBitmap(ctx, draw, de, dst)
	} else if !isNotFound(err) {
		return errValue(err)
	}
	if werr := h.writeBytes(ctx, dst, out, 0); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(maxLen)}
}

// deleteBitmap 删除 dest 旧值（含 hash sidecar，当 dest 原为 hash 时）。
func (h *bitmapHandler) deleteBitmap(ctx context.Context, raw []byte, e datastruct.Entry, userKey string) {
	_ = h.kv.Delete(ctx, raw)
	if e.Type == datastruct.TypeHash {
		_ = h.kv.Delete(ctx, datastruct.HashExpKey(userKey))
	}
}
