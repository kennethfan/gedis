package commands

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterHash 注册全部 hash 命令。
func RegisterHash(r *network.Router, kv KV) {
	h := &hashHandler{kv: kv}
	r.Register("HSET", h.hset)
	r.Register("HGET", h.hget)
	r.Register("HDEL", h.hdel)
	r.Register("HSETNX", h.hsetnx)
	r.Register("HEXISTS", h.hexists)
	r.Register("HLEN", h.hlen)
	h.registerMulti(r)
	h.registerIncr(r)
	h.registerHashExpire(r)
}

type hashHandler struct {
	kv KV
}

// readHash 读 hash key：不存在返回 ErrNotFound；已过期删除后返回
// ErrNotFound；类型非 hash 返回 wrongType 错误。附带过滤已过期 field
// （命中则回写清理，与 key 级被动过期同构）。
func (h *hashHandler) readHash(ctx context.Context, key string) (map[string]string, int64, error) {
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil {
		return nil, 0, err
	}
	if e.Type != datastruct.TypeHash {
		return nil, 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	m, _, err := datastruct.DecodeHash(e.Payload)
	if err != nil {
		return nil, 0, err
	}
	exp := h.readFieldExp(ctx, key)
	if len(exp) > 0 {
		now := time.Now().UnixNano()
		dropped := false
		for f, ts := range exp {
			if nano, perr := strconv.ParseInt(ts, 10, 64); perr != nil || now >= nano {
				delete(m, f)
				delete(exp, f)
				dropped = true
			}
		}
		if dropped {
			if werr := h.writeHash(ctx, key, m, e.Expiry); werr != nil {
				return nil, 0, werr
			}
			if werr := h.writeFieldExp(ctx, key, exp); werr != nil {
				return nil, 0, werr
			}
		}
	}
	return m, e.Expiry, nil
}

func (h *hashHandler) writeHash(ctx context.Context, key string, m map[string]string, expiry int64) error {
	return h.kv.Set(ctx, datastruct.HashKey(key), datastruct.Encode(datastruct.TypeHash, expiry, datastruct.EncodeHash(m)))
}

func hashKey(args []protocol.Value) (string, bool) {
	if len(args) < 1 {
		return "", false
	}
	return argString(args[0])
}

func (h *hashHandler) hset(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return errValueStr("ERR wrong number of arguments for 'hset' command")
	}
	key, ok := hashKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, expiry, err := h.readHash(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		m = make(map[string]string)
	}
	var added int64
	for i := 1; i < len(args); i += 2 {
		f, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR invalid field")
		}
		v, ok := argString(args[i+1])
		if !ok {
			return errValueStr("ERR invalid value")
		}
		if _, exists := m[f]; !exists {
			added++
		}
		m[f] = v
	}
	if err := h.writeHash(ctx, key, m, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: added}
}

func (h *hashHandler) hget(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'hget' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	field, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid field")
	}
	m, _, err := h.readHash(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	v, exists := m[field]
	if !exists {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	return protocol.BulkOf(v)
}

func (h *hashHandler) hdel(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'hdel' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, expiry, err := h.readHash(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	var deleted int64
	exp := h.readFieldExp(ctx, key)
	expDirty := false
	for _, a := range args[1:] {
		f, ok := argString(a)
		if !ok {
			continue
		}
		if _, exists := m[f]; exists {
			delete(m, f)
			deleted++
		}
		if _, ok := exp[f]; ok {
			delete(exp, f)
			expDirty = true
		}
	}
	if err := h.writeHash(ctx, key, m, expiry); err != nil {
		return errValue(err)
	}
	if expDirty {
		if err := h.writeFieldExp(ctx, key, exp); err != nil {
			return errValue(err)
		}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: deleted}
}

func (h *hashHandler) hsetnx(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'hsetnx' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	field, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid field")
	}
	value, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid value")
	}
	m, expiry, err := h.readHash(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		m = make(map[string]string)
	}
	if _, exists := m[field]; exists {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	m[field] = value
	if err := h.writeHash(ctx, key, m, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 1}
}

func (h *hashHandler) hexists(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'hexists' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	field, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid field")
	}
	m, _, err := h.readHash(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	if _, exists := m[field]; exists {
		return protocol.Value{Kind: protocol.KindInteger, I: 1}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 0}
}

func (h *hashHandler) hlen(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'hlen' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, _, err := h.readHash(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(m))}
}
