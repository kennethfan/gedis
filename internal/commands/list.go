package commands

import (
	"context"
	"fmt"
	"strconv"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterList 注册全部 list 命令；stats 为 nil 时 blocked_clients 不计数。
func RegisterList(r *network.Router, kv KV, stats *network.Stats) {
	h := &listHandler{kv: kv, stats: stats}
	r.Register("LPUSH", h.lpush)
	r.Register("RPUSH", h.rpush)
	r.Register("LPOP", h.lpop)
	r.Register("RPOP", h.rpop)
	r.Register("LLEN", h.llen)
	r.Register("LINDEX", h.lindex)
	r.Register("LRANGE", h.lrange)
	h.registerMod(r)
	h.registerBlock(r)
}

type listHandler struct {
	kv    KV
	stats *network.Stats
}

// readList 读 list key：不存在返回 ErrNotFound；已过期删除后返回
// ErrNotFound；类型非 list 返回 wrongType 错误。
func (h *listHandler) readList(ctx context.Context, key string) ([]string, int64, error) {
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil {
		return nil, 0, err
	}
	if e.Type != datastruct.TypeList {
		return nil, 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	elems, err := datastruct.DecodeList(e.Payload)
	if err != nil {
		return nil, 0, err
	}
	return elems, e.Expiry, nil
}

func (h *listHandler) writeList(ctx context.Context, key string, elems []string, expiry int64) error {
	return h.kv.Set(ctx, datastruct.ListKey(key), datastruct.Encode(datastruct.TypeList, expiry, datastruct.EncodeList(elems)))
}

func (h *listHandler) push(ctx context.Context, args []protocol.Value, head bool) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'push' command")
	}
	key, ok := listKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	elems, expiry, err := h.readList(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		elems = nil
	}
	for _, a := range args[1:] {
		v, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid value")
		}
		if head {
			elems = append([]string{v}, elems...)
		} else {
			elems = append(elems, v)
		}
	}
	if err := h.writeList(ctx, key, elems, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(elems))}
}

func (h *listHandler) lpush(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.push(ctx, args, true)
}

func (h *listHandler) rpush(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.push(ctx, args, false)
}

// popOne 弹出 count 个元素：tail=false 从头，true 从尾。
// 缺失 key 时单个返回 null bulk，带 count 返回空数组。
func (h *listHandler) popOne(ctx context.Context, args []protocol.Value, tail bool) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return errValueStr("ERR wrong number of arguments for 'pop' command")
	}
	key, ok := listKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	count := int64(1)
	withCount := false
	if len(args) == 2 {
		withCount = true
		c, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		n, err := strconv.ParseInt(c, 10, 64)
		if err != nil || n < 0 {
			return errValueStr("ERR value is not an integer or out of range")
		}
		count = n
	}
	elems, expiry, err := h.readList(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		if withCount {
			return toBulkArray(nil)
		}
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	if count > int64(len(elems)) {
		count = int64(len(elems))
	}
	var out []string
	if tail {
		out = elems[len(elems)-int(count):]
		elems = elems[:len(elems)-int(count)]
	} else {
		out = elems[:count]
		elems = elems[count:]
	}
	if len(elems) == 0 {
		if err := h.kv.Delete(ctx, datastruct.ListKey(key)); err != nil {
			return errValue(err)
		}
	} else if err := h.writeList(ctx, key, elems, expiry); err != nil {
		return errValue(err)
	}
	if !withCount {
		if len(out) == 0 {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return protocol.BulkOf(out[0])
	}
	return toBulkArray(out)
}

func (h *listHandler) lpop(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.popOne(ctx, args, false)
}

func (h *listHandler) rpop(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.popOne(ctx, args, true)
}

func (h *listHandler) llen(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'llen' command")
	}
	key, ok := listKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	elems, _, err := h.readList(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		return protocol.Value{Kind: protocol.KindInteger}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(elems))}
}

// normalizeIndex 把负索引归一化，越界返回 -1。
func normalizeIndex(idx, n int64) int64 {
	if idx < 0 {
		idx += n
	}
	if idx < 0 || idx >= n {
		return -1
	}
	return idx
}

func (h *listHandler) lindex(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'lindex' command")
	}
	key, ok := listKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	idxStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	idx, err := strconv.ParseInt(idxStr, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	elems, _, err := h.readList(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	pos := normalizeIndex(idx, int64(len(elems)))
	if pos < 0 {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	return protocol.BulkOf(elems[pos])
}

func (h *listHandler) lrange(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'lrange' command")
	}
	key, ok := listKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	startStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	stopStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(stopStr, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	elems, _, err := h.readList(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		return toBulkArray(nil)
	}
	n := int64(len(elems))
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return toBulkArray(nil)
	}
	return toBulkArray(elems[start : stop+1])
}

func listKey(args []protocol.Value) (string, bool) {
	if len(args) < 1 {
		return "", false
	}
	return argString(args[0])
}

// toBulkArray 把字符串切片转成 bulk 数组（空时返回空数组非 nil）。
func toBulkArray(elems []string) protocol.Value {
	out := make([]protocol.Value, len(elems))
	for i, e := range elems {
		out[i] = protocol.BulkOf(e)
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}
