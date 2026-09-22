package commands

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// blockPollInterval 是阻塞命令轮询 store 的间隔；事件唤醒留到 network 重构时做。
const blockPollInterval = 10 * time.Millisecond

func (h *listHandler) registerBlock(r *network.Router) {
	r.Register("BLPOP", h.blpop)
	r.Register("BRPOP", h.brpop)
	r.Register("BLMPOP", h.blmpop)
	r.Register("BRMPOP", h.brmpop)
	r.Register("LPOS", h.lpos)
}

// tryPopSingle 从 key 弹一个元素：tail=false 从头。缺失 key 返回 found=false。
func (h *listHandler) tryPopSingle(ctx context.Context, key string, tail bool) (elem string, found bool, err error) {
	elems, expiry, err := h.readList(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return "", false, nil
		}
		return "", false, err
	}
	if len(elems) == 0 {
		return "", false, nil
	}
	if tail {
		elem = elems[len(elems)-1]
		elems = elems[:len(elems)-1]
	} else {
		elem = elems[0]
		elems = elems[1:]
	}
	if len(elems) == 0 {
		if derr := h.kv.Delete(ctx, datastruct.ListKey(key)); derr != nil {
			return "", false, derr
		}
	} else if werr := h.writeList(ctx, key, elems, expiry); werr != nil {
		return "", false, werr
	}
	return elem, true, nil
}

// blockPop 轮询 keys 直到弹出元素、超时或 ctx 取消。
// timeout<=0 表示无限等待（只看 ctx）。
func (h *listHandler) blockPop(ctx context.Context, keys []string, tail bool, timeout time.Duration) (key, elem string, ok bool) {
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
		for _, k := range keys {
			e, found, err := h.tryPopSingle(ctx, k, tail)
			if err != nil {
				return "", "", false
			}
			if found {
				return k, e, true
			}
		}
		if timeout > 0 && !time.Now().Before(deadline) {
			return "", "", false
		}
		if !blocked {
			h.stats.IncBlocked()
			blocked = true
		}
		select {
		case <-ctx.Done():
			return "", "", false
		case <-time.After(blockPollInterval):
		}
	}
}

func parseTimeout(s string) (time.Duration, bool) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f < 0 {
		return 0, false
	}
	return time.Duration(f * float64(time.Second)), true
}

func (h *listHandler) blpop(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.bpop(ctx, args, false)
}

func (h *listHandler) brpop(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.bpop(ctx, args, true)
}

func (h *listHandler) bpop(ctx context.Context, args []protocol.Value, tail bool) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'bpop' command")
	}
	timeoutStr, ok := argString(args[len(args)-1])
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	timeout, ok := parseTimeout(timeoutStr)
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[:len(args)-1] {
		k, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid key")
		}
		keys = append(keys, k)
	}
	// 先按 keyspace 语义检查类型：命中非 list key 直接报 WRONGTYPE，不进入等待。
	for _, k := range keys {
		_, _, err := h.readList(ctx, k)
		if err != nil && !isNotFound(err) {
			return errValue(err)
		}
	}
	k, e, ok := h.blockPop(ctx, keys, tail, timeout)
	if !ok {
		return protocol.Value{Kind: protocol.KindArray}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{protocol.BulkOf(k), protocol.BulkOf(e)}}
}

// blmpop 实现 BLMPOP/BRMPOP：
// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n]。
func (h *listHandler) blmpop(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.bmpop(ctx, args, false)
}

func (h *listHandler) brmpop(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.bmpop(ctx, args, true)
}

func (h *listHandler) bmpop(ctx context.Context, args []protocol.Value, _ bool) protocol.Value {
	if len(args) < 4 {
		return errValueStr("ERR wrong number of arguments for 'bmpop' command")
	}
	timeoutStr, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	timeout, ok := parseTimeout(timeoutStr)
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	numkeysStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	numkeys, err := strconv.ParseInt(numkeysStr, 10, 64)
	if err != nil || numkeys <= 0 || int(numkeys) > len(args)-2 {
		return errValueStr("ERR value is not an integer or out of range")
	}
	keys := make([]string, 0, numkeys)
	for _, a := range args[2 : 2+numkeys] {
		k, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid key")
		}
		keys = append(keys, k)
	}
	rest := args[2+numkeys:]
	if len(rest) < 1 {
		return errValueStr("ERR syntax error")
	}
	dirStr, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	dir := strings.ToUpper(dirStr)
	if dir != "LEFT" && dir != "RIGHT" {
		return errValueStr("ERR syntax error")
	}
	tail := dir == "RIGHT"
	count := int64(1)
	if len(rest) > 1 {
		if len(rest) != 3 {
			return errValueStr("ERR syntax error")
		}
		opt, ok := argString(rest[1])
		if !ok || strings.ToUpper(opt) != "COUNT" {
			return errValueStr("ERR syntax error")
		}
		countStr, ok := argString(rest[2])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		count, err = strconv.ParseInt(countStr, 10, 64)
		if err != nil || count <= 0 {
			return errValueStr("ERR value is not an integer or out of range")
		}
	}
	for _, k := range keys {
		_, _, err := h.readList(ctx, k)
		if err != nil && !isNotFound(err) {
			return errValue(err)
		}
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
		for _, k := range keys {
			got, found, perr := h.tryPopCount(ctx, k, tail, count)
			if perr != nil {
				return errValue(perr)
			}
			if found {
				return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
					protocol.BulkOf(k),
					toBulkArray(got),
				}}
			}
		}
		if timeout > 0 && !time.Now().Before(deadline) {
			return protocol.Value{Kind: protocol.KindArray}
		}
		if !blocked {
			h.stats.IncBlocked()
			blocked = true
		}
		select {
		case <-ctx.Done():
			return protocol.Value{Kind: protocol.KindArray}
		case <-time.After(blockPollInterval):
		}
	}
}

// tryPopCount 从 key 弹最多 count 个元素。
func (h *listHandler) tryPopCount(ctx context.Context, key string, tail bool, count int64) ([]string, bool, error) {
	elems, expiry, err := h.readList(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(elems) == 0 {
		return nil, false, nil
	}
	n := count
	if n > int64(len(elems)) {
		n = int64(len(elems))
	}
	var out []string
	if tail {
		out = elems[len(elems)-int(n):]
		elems = elems[:len(elems)-int(n)]
	} else {
		out = elems[:n]
		elems = elems[n:]
	}
	if len(elems) == 0 {
		if derr := h.kv.Delete(ctx, datastruct.ListKey(key)); derr != nil {
			return nil, false, derr
		}
	} else if werr := h.writeList(ctx, key, elems, expiry); werr != nil {
		return nil, false, werr
	}
	return out, true, nil
}

func (h *listHandler) lpos(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'lpos' command")
	}
	key, ok := listKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	target, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid value")
	}
	rank := int64(1)
	count := int64(1)
	withCount := false
	maxlen := int64(0)
	i := 2
	for i < len(args) {
		opt, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		if i+1 >= len(args) {
			return errValueStr("ERR syntax error")
		}
		valStr, ok := argString(args[i+1])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		n, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
		switch strings.ToUpper(opt) {
		case "RANK":
			if n == 0 {
				return errValueStr("ERR RANK can't be zero")
			}
			rank = n
		case "COUNT":
			if n < 0 {
				return errValueStr("ERR COUNT can't be negative")
			}
			count = n
			withCount = true
		case "MAXLEN":
			if n < 0 {
				return errValueStr("ERR MAXLEN can't be negative")
			}
			maxlen = n
		default:
			return errValueStr("ERR syntax error")
		}
		i += 2
	}
	elems, _, err := h.readList(ctx, key)
	if err != nil {
		if isNotFound(err) {
			if withCount {
				return toIntArray(nil)
			}
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	limit := int64(len(elems))
	if maxlen > 0 && maxlen < limit {
		limit = maxlen
	}
	order := make([]int64, 0, limit)
	if rank > 0 {
		for idx := int64(0); idx < limit; idx++ {
			order = append(order, idx)
		}
	} else {
		for idx := int64(len(elems)) - 1; idx >= int64(len(elems))-limit; idx-- {
			order = append(order, idx)
		}
	}
	want := absCount(rank)
	var matched []int64
	var seen int64
	for _, idx := range order {
		if elems[idx] != target {
			continue
		}
		seen++
		if seen < want {
			continue
		}
		matched = append(matched, idx)
		if withCount && count > 0 && int64(len(matched)) >= count {
			break
		}
		if !withCount {
			break
		}
	}
	if !withCount {
		if len(matched) == 0 {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return protocol.Value{Kind: protocol.KindInteger, I: matched[0]}
	}
	return toIntArray(matched)
}

// toIntArray 把索引切片转成 integer 数组（空时返回空数组非 nil）。
func toIntArray(ns []int64) protocol.Value {
	out := make([]protocol.Value, len(ns))
	for i, n := range ns {
		out[i] = protocol.Value{Kind: protocol.KindInteger, I: n}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}
