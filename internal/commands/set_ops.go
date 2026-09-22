package commands

import (
	"context"
	"path"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *setHandler) registerOps(r *network.Router) {
	r.Register("SINTER", h.sinter)
	r.Register("SINTERSTORE", h.sinterstore)
	r.Register("SUNION", h.sunion)
	r.Register("SUNIONSTORE", h.sunionstore)
	r.Register("SDIFF", h.sdiff)
	r.Register("SDIFFSTORE", h.sdiffstore)
	r.Register("SSCAN", h.sscan)
}

// readSets 读多个 key 为集合：缺失 key 视为空集；任一类型错误返回 WRONGTYPE。
func (h *setHandler) readSets(ctx context.Context, keys []string) ([]map[string]struct{}, *protocol.Value) {
	sets := make([]map[string]struct{}, 0, len(keys))
	for _, k := range keys {
		set, _, err := h.readSet(ctx, k)
		if err != nil {
			if !isNotFound(err) {
				v := errValue(err)
				return nil, &v
			}
			set = make(map[string]struct{})
		}
		sets = append(sets, set)
	}
	return sets, nil
}

func setKeys(args []protocol.Value) ([]string, *protocol.Value) {
	keys := make([]string, 0, len(args))
	for _, a := range args {
		k, ok := argString(a)
		if !ok {
			v := errValueStr("ERR invalid key")
			return nil, &v
		}
		keys = append(keys, k)
	}
	return keys, nil
}

func (h *setHandler) sinter(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'sinter' command")
	}
	keys, errReply := setKeys(args)
	if errReply != nil {
		return *errReply
	}
	sets, errReply := h.readSets(ctx, keys)
	if errReply != nil {
		return *errReply
	}
	return toBulkArray(sortedMembers(h.interSets(sets)))
}

func (h *setHandler) sunion(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'sunion' command")
	}
	keys, errReply := setKeys(args)
	if errReply != nil {
		return *errReply
	}
	sets, errReply := h.readSets(ctx, keys)
	if errReply != nil {
		return *errReply
	}
	return toBulkArray(sortedMembers(h.unionSets(sets)))
}

func (h *setHandler) sdiff(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'sdiff' command")
	}
	keys, errReply := setKeys(args)
	if errReply != nil {
		return *errReply
	}
	sets, errReply := h.readSets(ctx, keys)
	if errReply != nil {
		return *errReply
	}
	return toBulkArray(sortedMembers(h.diffSets(sets)))
}

// storeResult 把集合运算结果写入目标 key：空结果删除目标 key；
// 非空覆盖写入（过期清零，视作新 key）。
func (h *setHandler) storeResult(ctx context.Context, dst string, result map[string]struct{}) protocol.Value {
	if len(result) == 0 {
		_ = h.kv.Delete(ctx, datastruct.SetKey(dst))
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	if err := h.writeSet(ctx, dst, result, 0); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(result))}
}

func (h *setHandler) sinterstore(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.storeSetOp(ctx, args, "sinterstore", h.interSets)
}

func (h *setHandler) sunionstore(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.storeSetOp(ctx, args, "sunionstore", h.unionSets)
}

func (h *setHandler) sdiffstore(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.storeSetOp(ctx, args, "sdiffstore", h.diffSets)
}

func (h *setHandler) interSets(sets []map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for m := range sets[0] {
		inAll := true
		for _, s := range sets[1:] {
			if _, ok := s[m]; !ok {
				inAll = false
				break
			}
		}
		if inAll {
			out[m] = struct{}{}
		}
	}
	return out
}

func (h *setHandler) unionSets(sets []map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for _, s := range sets {
		for m := range s {
			out[m] = struct{}{}
		}
	}
	return out
}

func (h *setHandler) diffSets(sets []map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for m := range sets[0] {
		excluded := false
		for _, s := range sets[1:] {
			if _, ok := s[m]; ok {
				excluded = true
				break
			}
		}
		if !excluded {
			out[m] = struct{}{}
		}
	}
	return out
}

func (h *setHandler) storeSetOp(ctx context.Context, args []protocol.Value, name string, op func([]map[string]struct{}) map[string]struct{}) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
	}
	dst, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid destination")
	}
	keys, errReply := setKeys(args[1:])
	if errReply != nil {
		return *errReply
	}
	sets, errReply := h.readSets(ctx, keys)
	if errReply != nil {
		return *errReply
	}
	return h.storeResult(ctx, dst, op(sets))
}

func (h *setHandler) sscan(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'sscan' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	cursorStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid cursor")
	}
	cursor, err := strconv.ParseInt(cursorStr, 10, 64)
	if err != nil || cursor < 0 {
		return errValueStr("ERR value is not an integer or out of range")
	}
	pattern := "*"
	var count int64 = 10
	for i := 2; i < len(args); i++ {
		name, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(name) {
		case "MATCH":
			i++
			if i >= len(args) {
				return errValueStr("ERR syntax error")
			}
			pattern, ok = argString(args[i])
			if !ok {
				return errValueStr("ERR syntax error")
			}
		case "COUNT":
			i++
			if i >= len(args) {
				return errValueStr("ERR syntax error")
			}
			n, ok := argString(args[i])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			count, err = strconv.ParseInt(n, 10, 64)
			if err != nil || count < 0 {
				return errValueStr("ERR value is not an integer or out of range")
			}
		default:
			return errValueStr("ERR syntax error")
		}
	}
	set, _, rerr := h.readSet(ctx, key)
	if rerr != nil {
		if isNotFound(rerr) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
				protocol.BulkOf("0"),
				{Kind: protocol.KindArray, Elems: []protocol.Value{}},
			}}
		}
		return errValue(rerr)
	}
	members := sortedMembers(set)
	matched := make([]string, 0, len(members))
	for _, m := range members {
		ok, merr := path.Match(pattern, m)
		if merr != nil || !ok {
			continue
		}
		matched = append(matched, m)
	}
	if cursor > int64(len(matched)) {
		cursor = int64(len(matched))
	}
	end := cursor + count
	var next int64
	if end >= int64(len(matched)) {
		end = int64(len(matched))
		next = 0
	} else {
		next = end
	}
	flat := make([]protocol.Value, 0, end-cursor)
	for _, m := range matched[cursor:end] {
		flat = append(flat, protocol.BulkOf(m))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(strconv.FormatInt(next, 10)),
		{Kind: protocol.KindArray, Elems: flat},
	}}
}
