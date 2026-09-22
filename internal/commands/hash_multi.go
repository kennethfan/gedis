package commands

import (
	"context"
	"path"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *hashHandler) registerMulti(r *network.Router) {
	r.Register("HMSET", h.hmset)
	r.Register("HMGET", h.hmget)
	r.Register("HGETALL", h.hgetall)
	r.Register("HKEYS", h.hkeys)
	r.Register("HVALS", h.hvals)
	r.Register("HSCAN", h.hscan)
}

func sortedHashFields(m map[string]string) []string {
	fields := make([]string, 0, len(m))
	for f := range m {
		fields = append(fields, f)
	}
	for i := 1; i < len(fields); i++ {
		for j := i; j > 0 && fields[j] < fields[j-1]; j-- {
			fields[j], fields[j-1] = fields[j-1], fields[j]
		}
	}
	return fields
}

func (h *hashHandler) hmset(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return errValueStr("ERR wrong number of arguments for 'hmset' command")
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
	for i := 1; i < len(args); i += 2 {
		f, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR invalid field")
		}
		v, ok := argString(args[i+1])
		if !ok {
			return errValueStr("ERR invalid value")
		}
		m[f] = v
	}
	if err := h.writeHash(ctx, key, m, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *hashHandler) hmget(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'hmget' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, _, err := h.readHash(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		m = nil
	}
	out := make([]protocol.Value, 0, len(args)-1)
	for _, a := range args[1:] {
		f, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid field")
		}
		v, exists := m[f]
		if !exists {
			out = append(out, protocol.Value{Kind: protocol.KindBulkString})
			continue
		}
		out = append(out, protocol.BulkOf(v))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *hashHandler) hgetall(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'hgetall' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, _, err := h.readHash(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	out := make([]protocol.Value, 0, 2*len(m))
	for _, f := range sortedHashFields(m) {
		out = append(out, protocol.BulkOf(f), protocol.BulkOf(m[f]))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *hashHandler) hkeys(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'hkeys' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, _, err := h.readHash(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	out := make([]protocol.Value, 0, len(m))
	for _, f := range sortedHashFields(m) {
		out = append(out, protocol.BulkOf(f))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *hashHandler) hvals(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'hvals' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, _, err := h.readHash(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	out := make([]protocol.Value, 0, len(m))
	for _, f := range sortedHashFields(m) {
		out = append(out, protocol.BulkOf(m[f]))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *hashHandler) hscan(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'hscan' command")
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
	m, _, rerr := h.readHash(ctx, key)
	if rerr != nil {
		if isNotFound(rerr) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
				protocol.BulkOf("0"),
				{Kind: protocol.KindArray, Elems: []protocol.Value{}},
			}}
		}
		return errValue(rerr)
	}
	fields := sortedHashFields(m)
	matched := make([]string, 0, len(fields))
	for _, f := range fields {
		ok, merr := path.Match(pattern, f)
		if merr != nil || !ok {
			continue
		}
		matched = append(matched, f)
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
	flat := make([]protocol.Value, 0, 2*(end-cursor))
	for _, f := range matched[cursor:end] {
		flat = append(flat, protocol.BulkOf(f), protocol.BulkOf(m[f]))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(strconv.FormatInt(next, 10)),
		{Kind: protocol.KindArray, Elems: flat},
	}}
}
