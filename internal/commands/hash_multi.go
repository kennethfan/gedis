package commands

import (
	"context"
	"math/rand"
	"path"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

var hashMultiMeta = []acl.Meta{
	{Name: "HMSET", Category: "hash", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "HMGET", Category: "hash", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "HGETALL", Category: "hash", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "HKEYS", Category: "hash", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "HVALS", Category: "hash", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "HSCAN", Category: "hash", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "HRANDFIELD", Category: "hash", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
}

func (h *hashHandler) registerMulti(r *network.Router) {
	for _, m := range hashMultiMeta {
		acl.RegisterMeta(m)
	}
	r.Register("HMSET", h.hmset)
	r.Register("HMGET", h.hmget)
	r.Register("HGETALL", h.hgetall)
	r.Register("HKEYS", h.hkeys)
	r.Register("HVALS", h.hvals)
	r.Register("HSCAN", h.hscan)
	r.Register("HRANDFIELD", h.hrandfield)
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

// hrandfield 实现 HRANDFIELD key [count [WITHVALUES]]：无 count 取单个；
// 正 count 去重取 min(count,len)；负 count 可重复取 abs(count)。
func (h *hashHandler) hrandfield(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 3 {
		return errValueStr("ERR wrong number of arguments for 'hrandfield' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, _, err := h.readHash(ctx, key)
	if err != nil {
		if isNotFound(err) {
			if len(args) == 1 {
				return protocol.Value{Kind: protocol.KindBulkString}
			}
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	withValues := false
	count := int64(0)
	counted := false
	if len(args) >= 2 {
		cstr, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		count, err = strconv.ParseInt(cstr, 10, 64)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
		counted = true
	}
	if len(args) == 3 {
		w, ok := argString(args[2])
		if !ok || !strings.EqualFold(w, "WITHVALUES") {
			return errValueStr("ERR syntax error")
		}
		withValues = true
	}
	fields := make([]string, 0, len(m))
	for f := range m {
		fields = append(fields, f)
	}
	if !counted {
		if len(fields) == 0 {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return protocol.BulkOf(fields[rand.Intn(len(fields))])
	}
	pick := func() string { return fields[rand.Intn(len(fields))] }
	out := make([]protocol.Value, 0)
	if count >= 0 {
		n := int(count)
		if n > len(fields) {
			n = len(fields)
		}
		perm := rand.Perm(len(fields))[:n]
		for _, i := range perm {
			out = append(out, hrandfieldElem(m, fields[i], withValues))
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	}
	for i := int64(0); i < -count; i++ {
		out = append(out, hrandfieldElem(m, pick(), withValues))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func hrandfieldElem(m map[string]string, f string, withValues bool) protocol.Value {
	if !withValues {
		return protocol.BulkOf(f)
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(f), protocol.BulkOf(m[f]),
	}}
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
	noValues := false
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
		case "NOVALUES":
			noValues = true
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
		flat = append(flat, protocol.BulkOf(f))
		if !noValues {
			flat = append(flat, protocol.BulkOf(m[f]))
		}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(strconv.FormatInt(next, 10)),
		{Kind: protocol.KindArray, Elems: flat},
	}}
}
