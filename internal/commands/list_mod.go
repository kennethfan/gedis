package commands

import (
	"context"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *listHandler) registerMod(r *network.Router) {
	r.Register("LSET", h.lset)
	r.Register("LINSERT", h.linsert)
	r.Register("LREM", h.lrem)
	r.Register("LTRIM", h.ltrim)
}

func (h *listHandler) lset(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'lset' command")
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
	val, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid value")
	}
	elems, expiry, err := h.readList(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return errValueStr("ERR no such key")
		}
		return errValue(err)
	}
	pos := normalizeIndex(idx, int64(len(elems)))
	if pos < 0 {
		return errValueStr("ERR index out of range")
	}
	elems[pos] = val
	if err := h.writeList(ctx, key, elems, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *listHandler) linsert(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 4 {
		return errValueStr("ERR wrong number of arguments for 'linsert' command")
	}
	key, ok := listKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	where, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	where = strings.ToUpper(where)
	if where != "BEFORE" && where != "AFTER" {
		return errValueStr("ERR syntax error")
	}
	pivot, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid pivot")
	}
	val, ok := argString(args[3])
	if !ok {
		return errValueStr("ERR invalid value")
	}
	elems, expiry, err := h.readList(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	pos := -1
	for i, e := range elems {
		if e == pivot {
			pos = i
			break
		}
	}
	if pos < 0 {
		return protocol.Value{Kind: protocol.KindInteger, I: -1}
	}
	if where == "AFTER" {
		pos++
	}
	elems = append(elems, "")
	copy(elems[pos+1:], elems[pos:])
	elems[pos] = val
	if err := h.writeList(ctx, key, elems, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(elems))}
}

func (h *listHandler) lrem(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'lrem' command")
	}
	key, ok := listKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	countStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	val, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid value")
	}
	elems, expiry, err := h.readList(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	var kept []string
	var removed int64
	match := func() bool {
		if count == 0 || removed < absCount(count) {
			return true
		}
		return false
	}
	if count >= 0 {
		for _, e := range elems {
			if e == val && match() {
				removed++
				continue
			}
			kept = append(kept, e)
		}
	} else {
		for i := len(elems) - 1; i >= 0; i-- {
			if elems[i] == val && match() {
				removed++
				continue
			}
			kept = append([]string{elems[i]}, kept...)
		}
	}
	if len(kept) == 0 && removed > 0 {
		_ = h.kv.Delete(ctx, datastruct.ListKey(key))
		return protocol.Value{Kind: protocol.KindInteger, I: removed}
	}
	if removed > 0 {
		if err := h.writeList(ctx, key, kept, expiry); err != nil {
			return errValue(err)
		}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: removed}
}

func absCount(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func (h *listHandler) ltrim(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'ltrim' command")
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
	elems, expiry, err := h.readList(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
		}
		return errValue(err)
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
	var kept []string
	if !(start > stop || start >= n) {
		kept = elems[start : stop+1]
	}
	if len(kept) == 0 {
		_ = h.kv.Delete(ctx, datastruct.ListKey(key))
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	}
	if err := h.writeList(ctx, key, kept, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}
