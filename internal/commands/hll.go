package commands

import (
	"context"
	"fmt"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
)

// RegisterHLL 注册 HyperLogLog 命令；新 hll: 前缀，dense-only。
func RegisterHLL(r *network.Router, kv KV) {
	h := &hllHandler{kv: kv}
	r.Register("PFADD", h.pfadd)
	r.Register("PFCOUNT", h.pfcount)
	r.Register("PFMERGE", h.pfmerge)
}

type hllHandler struct {
	kv KV
}

func hllWrongType() error {
	return fmt.Errorf("WRONGTYPE Key is not a valid HyperLogLog string value.")
}

// readHLL 读 HLL：不存在返回 ErrNotFound；类型错报 HLL 专用 WRONGTYPE。
func (h *hllHandler) readHLL(ctx context.Context, key string) ([]byte, int64, error) {
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil {
		return nil, 0, err
	}
	if e.Type != datastruct.TypeHLL {
		return nil, 0, hllWrongType()
	}
	reg, err := datastruct.DecodeHLL(e.Payload)
	if err != nil {
		return nil, 0, hllWrongType()
	}
	return reg, e.Expiry, nil
}

func (h *hllHandler) writeHLL(ctx context.Context, key string, reg []byte, expiry int64) error {
	return h.kv.Set(ctx, datastruct.HLLKey(key), datastruct.Encode(datastruct.TypeHLL, expiry, datastruct.EncodeHLL(reg)))
}

func (h *hllHandler) pfadd(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'pfadd' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	reg, expiry, err := h.readHLL(ctx, key)
	created := false
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		reg = datastruct.HLLNewDense()
		created = true
	}
	changed := false
	for _, a := range args[1:] {
		el, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid element")
		}
		if datastruct.HLLDenseAdd(reg, []byte(el)) {
			changed = true
		}
	}
	if werr := h.writeHLL(ctx, key, reg, expiry); werr != nil {
		return errValue(werr)
	}
	if changed || created {
		return protocol.Value{Kind: protocol.KindInteger, I: 1}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 0}
}

func (h *hllHandler) pfcount(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'pfcount' command")
	}
	if len(args) == 1 {
		key, ok := argString(args[0])
		if !ok {
			return errValueStr("ERR invalid key")
		}
		reg, _, err := h.readHLL(ctx, key)
		if err != nil {
			if isNotFound(err) {
				return protocol.Value{Kind: protocol.KindInteger, I: 0}
			}
			return errValue(err)
		}
		return protocol.Value{Kind: protocol.KindInteger, I: int64(datastruct.HLLDenseCount(reg))}
	}
	union := datastruct.HLLNewDense()
	found := false
	for _, a := range args {
		key, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid key")
		}
		reg, _, err := h.readHLL(ctx, key)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return errValue(err)
		}
		datastruct.HLLDenseMerge(union, reg)
		found = true
	}
	if !found {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(datastruct.HLLDenseCount(union))}
}

func (h *hllHandler) pfmerge(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'pfmerge' command")
	}
	dst, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	var dstRegs []byte
	var dstExpiry int64
	if e, err := lookupKey(ctx, h.kv, dst); err == nil {
		if e.Type != datastruct.TypeHLL {
			return errValue(hllWrongType())
		}
		dstRegs, err = datastruct.DecodeHLL(e.Payload)
		if err != nil {
			return errValue(err)
		}
		dstExpiry = e.Expiry
	} else if !isNotFound(err) {
		return errValue(err)
	}
	merged := datastruct.HLLNewDense()
	if dstRegs != nil {
		datastruct.HLLDenseMerge(merged, dstRegs)
	}
	for _, a := range args[1:] {
		key, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid key")
		}
		reg, _, err := h.readHLL(ctx, key)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return errValue(err)
		}
		datastruct.HLLDenseMerge(merged, reg)
	}
	if werr := h.kv.WriteBatch(ctx, []storage.BatchOp{
		{Key: datastruct.HLLKey(dst), Value: datastruct.Encode(datastruct.TypeHLL, dstExpiry, datastruct.EncodeHLL(merged))},
	}); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}
