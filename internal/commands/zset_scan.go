package commands

import (
	"context"
	"math/rand"
	"path"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *zsetHandler) registerScanZ(r *network.Router) {
	r.Register("ZRANDMEMBER", h.zrandmember)
	r.Register("ZSCAN", h.zscan)
}

// zrandmember 实现 ZRANDMEMBER key [count [WITHSCORES]]。
func (h *zsetHandler) zrandmember(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 3 {
		return errValueStr("ERR wrong number of arguments for 'zrandmember' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	hasCount := len(args) >= 2
	var count int64
	withScores := false
	if hasCount {
		cstr, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR invalid count")
		}
		var ok2 bool
		count, ok2 = parseCount(cstr)
		if !ok2 && !isNegCount(cstr) {
			return errValueStr("ERR value is not an integer or out of range")
		}
		if len(args) == 3 {
			opt, ok := argString(args[2])
			if !ok || !strings.EqualFold(opt, "WITHSCORES") {
				return errValueStr("ERR syntax error")
			}
			withScores = true
		}
	}
	z, _, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			if hasCount {
				return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
			}
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	members := sortedZSet(z)
	if !hasCount {
		if len(members) == 0 {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return protocol.BulkOf(members[rand.Intn(len(members))].m)
	}
	if count == 0 {
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	}
	emit := func(p zsetMember) protocol.Value {
		if withScores {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
				protocol.BulkOf(p.m), protocol.BulkOf(formatScore(p.s)),
			}}
		}
		return protocol.BulkOf(p.m)
	}
	if count > 0 {
		if count >= int64(len(members)) {
			out := make([]protocol.Value, 0, len(members))
			for _, p := range members {
				out = append(out, emit(p))
			}
			return protocol.Value{Kind: protocol.KindArray, Elems: out}
		}
		perm := rand.Perm(len(members))
		out := make([]protocol.Value, 0, count)
		for _, i := range perm[:count] {
			out = append(out, emit(members[i]))
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	}
	n := -count
	out := make([]protocol.Value, 0, n)
	for i := int64(0); i < n; i++ {
		out = append(out, emit(members[rand.Intn(len(members))]))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func isNegCount(s string) bool {
	n, err := strconv.ParseInt(s, 10, 64)
	return err == nil && n < 0
}

// zscan 实现 ZSCAN key cursor [MATCH pattern] [COUNT n]，返回 member+score 对。
func (h *zsetHandler) zscan(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'zscan' command")
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
	empty := protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("0"),
		{Kind: protocol.KindArray, Elems: []protocol.Value{}},
	}}
	z, _, rerr := h.readZSet(ctx, key)
	if rerr != nil {
		if isNotFound(rerr) {
			return empty
		}
		return errValue(rerr)
	}
	members := sortedZSet(z)
	matched := make([]zsetMember, 0, len(members))
	for _, p := range members {
		ok, merr := path.Match(pattern, p.m)
		if merr != nil || !ok {
			continue
		}
		matched = append(matched, p)
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
	flat := make([]protocol.Value, 0, (end-cursor)*2)
	for _, p := range matched[cursor:end] {
		flat = append(flat, protocol.BulkOf(p.m), protocol.BulkOf(formatScore(p.s)))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(strconv.FormatInt(next, 10)),
		{Kind: protocol.KindArray, Elems: flat},
	}}
}
