package commands

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/datastruct"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

var listModMeta = []acl.Meta{
	{Name: "LSET", Category: "list", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "LINSERT", Category: "list", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "LREM", Category: "list", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "LTRIM", Category: "list", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "LMOVE", Category: "list", Keys: acl.KeySpec{First: 0, Last: 1}},
	{Name: "BLMOVE", Category: "list", Keys: acl.KeySpec{First: 0, Last: 1}},
	{Name: "RPOPLPUSH", Category: "list", Keys: acl.KeySpec{First: 0, Last: 1}},
	{Name: "BRPOPLPUSH", Category: "list", Keys: acl.KeySpec{First: 0, Last: 1}},
}

func (h *listHandler) registerMod(r *network.Router) {
	for _, m := range listModMeta {
		acl.RegisterMeta(m)
	}
	r.Register("LSET", h.lset)
	r.Register("LINSERT", h.linsert)
	r.Register("LREM", h.lrem)
	r.Register("LTRIM", h.ltrim)
	r.Register("LMOVE", h.lmove)
	r.Register("BLMOVE", h.blmove)
	r.Register("RPOPLPUSH", h.rpoplpush)
	r.Register("BRPOPLPUSH", h.brpoplpush)
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
	Notify("l", "lset", key)
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
	Notify("l", "linsert", key)
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
		Notify("l", "lrem", key)
		Notify("g", "del", key)
		return protocol.Value{Kind: protocol.KindInteger, I: removed}
	}
	if removed > 0 {
		if err := h.writeList(ctx, key, kept, expiry); err != nil {
			return errValue(err)
		}
		Notify("l", "lrem", key)
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
		Notify("l", "ltrim", key)
		Notify("g", "del", key)
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	}
	if err := h.writeList(ctx, key, kept, expiry); err != nil {
		return errValue(err)
	}
	Notify("l", "ltrim", key)
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

// parseMoveSide 把 LEFT/RIGHT 归一为"是否从尾部操作"（RIGHT=tail, LEFT=head）。
func parseMoveSide(s string) (tail bool, ok bool) {
	switch strings.ToUpper(s) {
	case "LEFT":
		return false, true
	case "RIGHT":
		return true, true
	}
	return false, false
}

// parseMoveTimeout 解析阻塞秒数：非法报 not a float，负数报 is negative。
func parseMoveTimeout(s string) (time.Duration, *protocol.Value) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, errPtr("ERR timeout is not a float or out of range")
	}
	if f < 0 {
		return 0, errPtr("ERR timeout is negative")
	}
	return time.Duration(f * float64(time.Second)), nil
}

// moveCore 执行一次原子"源弹出→目标压入"：push 事件先于 pop 事件发布，
// 源弹空时删除源 key 并补发 del；源空返回 null bulk。
func (h *listHandler) moveCore(ctx context.Context, src, dst string, fromTail, toTail bool) protocol.Value {
	srcElems, srcExp, err := h.readList(ctx, src)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	if len(srcElems) == 0 {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	var elem string
	if fromTail {
		elem = srcElems[len(srcElems)-1]
		srcElems = srcElems[:len(srcElems)-1]
	} else {
		elem = srcElems[0]
		srcElems = srcElems[1:]
	}
	dstElems, dstExp, err := h.readList(ctx, dst)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		dstElems, dstExp = nil, 0
	}
	if toTail {
		dstElems = append(dstElems, elem)
	} else {
		dstElems = append([]string{elem}, dstElems...)
	}
	pushEvent := "lpush"
	if toTail {
		pushEvent = "rpush"
	}
	popEvent := "lpop"
	if fromTail {
		popEvent = "rpop"
	}
	if src == dst {
		if err := h.writeList(ctx, dst, dstElems, srcExp); err != nil {
			return errValue(err)
		}
		Notify("l", pushEvent, dst)
		Notify("l", popEvent, src)
		return protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(elem)}
	}
	if err := h.writeList(ctx, dst, dstElems, dstExp); err != nil {
		return errValue(err)
	}
	Notify("l", pushEvent, dst)
	if len(srcElems) == 0 {
		if err := h.kv.Delete(ctx, datastruct.ListKey(src)); err != nil {
			return errValue(err)
		}
	} else if err := h.writeList(ctx, src, srcElems, srcExp); err != nil {
		return errValue(err)
	}
	Notify("l", popEvent, src)
	if len(srcElems) == 0 {
		Notify("g", "del", src)
	}
	return protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(elem)}
}

func (h *listHandler) lmove(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 4 {
		return errValueStr("ERR wrong number of arguments for 'lmove' command")
	}
	src, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	dst, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	fromStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	fromTail, ok := parseMoveSide(fromStr)
	if !ok {
		return errValueStr("ERR syntax error")
	}
	toStr, ok := argString(args[3])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	toTail, ok := parseMoveSide(toStr)
	if !ok {
		return errValueStr("ERR syntax error")
	}
	return h.moveCore(ctx, src, dst, fromTail, toTail)
}

// blockMove 轮询等待源可用后执行 moveCore；timeout<=0 无限等待。
func (h *listHandler) blockMove(ctx context.Context, src, dst string, fromTail, toTail bool, timeout time.Duration) protocol.Value {
	if _, _, err := h.readList(ctx, src); err != nil && !isNotFound(err) {
		return errValue(err)
	}
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	blocked := false
	defer func() {
		if blocked {
			h.stats.DecBlocked()
		}
	}()
	for {
		res := h.moveCore(ctx, src, dst, fromTail, toTail)
		if res.Kind != protocol.KindBulkString || len(res.Bulk) > 0 {
			return res
		}
		if timeout > 0 && !time.Now().Before(deadline) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		if !blocked {
			h.stats.IncBlocked()
			blocked = true
		}
		select {
		case <-ctx.Done():
			return protocol.Value{Kind: protocol.KindBulkString}
		case <-time.After(blockPollInterval):
		}
	}
}

func (h *listHandler) blmove(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 5 {
		return errValueStr("ERR wrong number of arguments for 'blmove' command")
	}
	src, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	dst, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	fromStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	fromTail, ok := parseMoveSide(fromStr)
	if !ok {
		return errValueStr("ERR syntax error")
	}
	toStr, ok := argString(args[3])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	toTail, ok := parseMoveSide(toStr)
	if !ok {
		return errValueStr("ERR syntax error")
	}
	ts, ok := argString(args[4])
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	timeout, errReply := parseMoveTimeout(ts)
	if errReply != nil {
		return *errReply
	}
	return h.blockMove(ctx, src, dst, fromTail, toTail, timeout)
}

func (h *listHandler) rpoplpush(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'rpoplpush' command")
	}
	src, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	dst, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	return h.moveCore(ctx, src, dst, true, false)
}

func (h *listHandler) brpoplpush(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'brpoplpush' command")
	}
	src, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	dst, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	ts, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	timeout, errReply := parseMoveTimeout(ts)
	if errReply != nil {
		return *errReply
	}
	return h.blockMove(ctx, src, dst, true, false, timeout)
}
