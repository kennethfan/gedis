package commands

import (
	"context"
	"fmt"
	"strconv"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterBitmap 注册 Bitmap 命令；复用 s: 前缀的 string 字节，bit 视图与 string 视图同体。
func RegisterBitmap(r *network.Router, kv KV) {
	h := &bitmapHandler{kv: kv}
	r.Register("SETBIT", h.setbit)
	r.Register("GETBIT", h.getbit)
	h.registerCount(r)
	h.registerOp(r)
	h.registerField(r)
}

type bitmapHandler struct {
	kv KV
}

// readBytes 读 string 字节：不存在返回空；非 string 报 WRONGTYPE；附带 expiry。
func (h *bitmapHandler) readBytes(ctx context.Context, key string) ([]byte, int64, error) {
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil {
		if isNotFound(err) {
			return nil, 0, err
		}
		return nil, 0, err
	}
	if e.Type != datastruct.TypeString {
		return nil, 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	out := append([]byte(nil), e.Payload...)
	return out, e.Expiry, nil
}

func (h *bitmapHandler) writeBytes(ctx context.Context, key string, buf []byte, expiry int64) error {
	return h.kv.Set(ctx, datastruct.StringKey(key), datastruct.EncodeString(buf, expiry))
}

// bitOffset 解析 bit 位移：[0, 2^32)。
func bitOffset(s string) (uint64, bool) {
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, false
	}
	return n, true
}

// getBitAt 按大端 bit 序读位；越界为 0。
func getBitAt(buf []byte, off uint64) int64 {
	i := off / 8
	if i >= uint64(len(buf)) {
		return 0
	}
	if buf[i]&(0x80>>(off%8)) != 0 {
		return 1
	}
	return 0
}

// setBitAt 按大端 bit 序写位，自动零扩展；返回旧值。
func setBitAt(buf []byte, off uint64, v byte) ([]byte, int64) {
	i := off / 8
	for uint64(len(buf)) <= i {
		buf = append(buf, 0)
	}
	old := getBitAt(buf, off)
	if v == 1 {
		buf[i] |= 0x80 >> (off % 8)
	} else {
		buf[i] &^= 0x80 >> (off % 8)
	}
	return buf, old
}

func (h *bitmapHandler) setbit(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'setbit' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	offStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR bit offset is not an integer or out of range")
	}
	off, valid := bitOffset(offStr)
	if !valid {
		return errValueStr("ERR bit offset is not an integer or out of range")
	}
	valStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR bit is not an integer or out of range")
	}
	if valStr != "0" && valStr != "1" {
		return errValueStr("ERR bit is not an integer or out of range")
	}
	buf, expiry, err := h.readBytes(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		buf = nil
	}
	var v byte
	if valStr == "1" {
		v = 1
	}
	buf, old := setBitAt(buf, off, v)
	if werr := h.writeBytes(ctx, key, buf, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: old}
}

func (h *bitmapHandler) getbit(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'getbit' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	offStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR bit offset is not an integer or out of range")
	}
	off, valid := bitOffset(offStr)
	if !valid {
		return errValueStr("ERR bit offset is not an integer or out of range")
	}
	buf, _, err := h.readBytes(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: getBitAt(buf, off)}
}

