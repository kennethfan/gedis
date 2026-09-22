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

func (h *hashHandler) registerHashExpire(r *network.Router) {
	r.Register("HEXPIRE", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return h.hexpire(ctx, args, expireRelativeSec)
	})
	r.Register("HEXPIREAT", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return h.hexpire(ctx, args, expireAbsoluteSec)
	})
	r.Register("HPEXPIRE", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return h.hexpire(ctx, args, expireRelativeMs)
	})
	r.Register("HPEXPIREAT", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return h.hexpire(ctx, args, expireAbsoluteMs)
	})
	r.Register("HTTL", h.httl)
	r.Register("HPTTL", h.hpttl)
	r.Register("HPERSIST", h.hpersist)
}

// readFieldExp 读 field 过期表 {field: unixnano}；无 sidecar 返回空表。
func (h *hashHandler) readFieldExp(ctx context.Context, key string) map[string]string {
	raw, err := h.kv.Get(ctx, datastruct.HashExpKey(key))
	if err != nil {
		return map[string]string{}
	}
	e, err := datastruct.Decode(raw)
	if err != nil {
		return map[string]string{}
	}
	exp, _, err := datastruct.DecodeHash(e.Payload)
	if err != nil {
		return map[string]string{}
	}
	return exp
}

func (h *hashHandler) writeFieldExp(ctx context.Context, key string, exp map[string]string) error {
	if len(exp) == 0 {
		return h.kv.Delete(ctx, datastruct.HashExpKey(key))
	}
	return h.kv.Set(ctx, datastruct.HashExpKey(key), datastruct.Encode(datastruct.TypeHash, 0, datastruct.EncodeHash(exp)))
}

type hexpireCond int

const (
	hexpireNone hexpireCond = iota
	hexpireNX
	hexpireXX
	hexpireGT
	hexpireLT
)

// hexpire 实现 HEXPIRE/HEXPIREAT/HPEXPIRE/HPEXPIREAT：
// HEXPIRE key ttl [NX|XX|GT|LT] FIELDS n field... → 每 field 一个整数回复
// （1 设置，0 条件不满足，-2 无此 field/key）。
func (h *hashHandler) hexpire(ctx context.Context, args []protocol.Value, kind expireKind) protocol.Value {
	if len(args) < 4 {
		return errValueStr("ERR wrong number of arguments for 'hexpire' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	timeoutStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	n, err := strconv.ParseInt(timeoutStr, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	cond := hexpireNone
	idx := 2
	if name, ok := argString(args[2]); ok {
		switch strings.ToUpper(name) {
		case "NX":
			cond = hexpireNX
			idx = 3
		case "XX":
			cond = hexpireXX
			idx = 3
		case "GT":
			cond = hexpireGT
			idx = 3
		case "LT":
			cond = hexpireLT
			idx = 3
		}
	}
	if idx >= len(args) {
		return errValueStr("ERR syntax error")
	}
	fieldsKw, ok := argString(args[idx])
	if !ok || strings.ToUpper(fieldsKw) != "FIELDS" {
		return errValueStr("ERR syntax error")
	}
	if idx+1 >= len(args) {
		return errValueStr("ERR syntax error")
	}
	countStr, ok := argString(args[idx+1])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil || count <= 0 || idx+2+int(count) != len(args) {
		return errValueStr("ERR syntax error")
	}

	var exp int64
	switch kind {
	case expireRelativeSec:
		exp = time.Now().Add(time.Duration(n) * time.Second).UnixNano()
	case expireRelativeMs:
		exp = time.Now().Add(time.Duration(n) * time.Millisecond).UnixNano()
	case expireAbsoluteSec:
		exp = time.Unix(n, 0).UnixNano()
	case expireAbsoluteMs:
		exp = time.Unix(0, n*int64(time.Millisecond)).UnixNano()
	}

	m, expiry, err := h.readHash(ctx, key)
	if err != nil {
		if isNotFound(err) {
			out := make([]protocol.Value, 0, count)
			for i := int64(0); i < count; i++ {
				out = append(out, protocol.Value{Kind: protocol.KindInteger, I: -2})
			}
			return protocol.Value{Kind: protocol.KindArray, Elems: out}
		}
		return errValue(err)
	}
	sidecar := h.readFieldExp(ctx, key)
	now := time.Now().UnixNano()
	out := make([]protocol.Value, 0, count)
	dirtyMain := false
	dirtyExp := false
	for _, a := range args[idx+2:] {
		f, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid field")
		}
		if _, exists := m[f]; !exists {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: -2})
			continue
		}
		curStr, hasExp := sidecar[f]
		var cur int64
		if hasExp {
			cur, _ = strconv.ParseInt(curStr, 10, 64)
		}
		apply := true
		switch cond {
		case hexpireNX:
			apply = !hasExp
		case hexpireXX:
			apply = hasExp
		case hexpireGT:
			apply = hasExp && exp > cur
		case hexpireLT:
			apply = !hasExp || exp < cur
		}
		if !apply {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: 0})
			continue
		}
		if exp <= now {
			delete(m, f)
			delete(sidecar, f)
			dirtyMain = true
			dirtyExp = true
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: 1})
			continue
		}
		sidecar[f] = strconv.FormatInt(exp, 10)
		dirtyExp = true
		out = append(out, protocol.Value{Kind: protocol.KindInteger, I: 1})
	}
	if dirtyMain {
		if err := h.writeHash(ctx, key, m, expiry); err != nil {
			return errValue(err)
		}
	}
	if dirtyExp {
		if err := h.writeFieldExp(ctx, key, sidecar); err != nil {
			return errValue(err)
		}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *hashHandler) httl(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.hfieldttl(ctx, args, false)
}

func (h *hashHandler) hpttl(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.hfieldttl(ctx, args, true)
}

// hfieldttl 实现 HTTL/HPTTL：-2 无 field/key，-1 无过期，否则剩余额度。
func (h *hashHandler) hfieldttl(ctx context.Context, args []protocol.Value, millis bool) protocol.Value {
	fields, errReply := httlFields(args)
	if errReply != nil {
		return *errReply
	}
	m, _, err := h.readHash(ctx, fields.key)
	if err != nil {
		if isNotFound(err) {
			return httlMissing(len(fields.names))
		}
		return errValue(err)
	}
	sidecar := h.readFieldExp(ctx, fields.key)
	now := time.Now().UnixNano()
	out := make([]protocol.Value, 0, len(fields.names))
	for _, f := range fields.names {
		if _, exists := m[f]; !exists {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: -2})
			continue
		}
		ts, ok := sidecar[f]
		if !ok {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: -1})
			continue
		}
		nano, perr := strconv.ParseInt(ts, 10, 64)
		if perr != nil || now >= nano {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: -2})
			continue
		}
		if millis {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: (nano - now) / int64(time.Millisecond)})
		} else {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: ((nano-now)/int64(time.Millisecond) + 500) / 1000})
		}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

type ttlFields struct {
	key   string
	names []string
}

func httlFields(args []protocol.Value) (ttlFields, *protocol.Value) {
	if len(args) < 2 {
		v := errValueStr("ERR wrong number of arguments")
		return ttlFields{}, &v
	}
	key, ok := argString(args[0])
	if !ok {
		v := errValueStr("ERR invalid key")
		return ttlFields{}, &v
	}
	rest := args[1:]
	if kw, ok := argString(args[1]); ok && strings.ToUpper(kw) == "FIELDS" {
		if len(args) < 3 {
			v := errValueStr("ERR syntax error")
			return ttlFields{}, &v
		}
		countStr, ok := argString(args[2])
		if !ok {
			v := errValueStr("ERR syntax error")
			return ttlFields{}, &v
		}
		count, err := strconv.ParseInt(countStr, 10, 64)
		if err != nil || count <= 0 || int(count) != len(args)-3 {
			v := errValueStr("ERR syntax error")
			return ttlFields{}, &v
		}
		rest = args[3:]
	}
	names := make([]string, 0, len(rest))
	for _, a := range rest {
		f, ok := argString(a)
		if !ok {
			v := errValueStr("ERR invalid field")
			return ttlFields{}, &v
		}
		names = append(names, f)
	}
	return ttlFields{key: key, names: names}, nil
}

func httlMissing(n int) protocol.Value {
	out := make([]protocol.Value, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, protocol.Value{Kind: protocol.KindInteger, I: -2})
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

// hpersist 实现 HPERSIST：1 已移除，-1 本无过期，-2 无 field。
func (h *hashHandler) hpersist(ctx context.Context, args []protocol.Value) protocol.Value {
	fields, errReply := httlFields(args)
	if errReply != nil {
		return *errReply
	}
	m, _, err := h.readHash(ctx, fields.key)
	if err != nil {
		if isNotFound(err) {
			return httlMissing(len(fields.names))
		}
		return errValue(err)
	}
	sidecar := h.readFieldExp(ctx, fields.key)
	out := make([]protocol.Value, 0, len(fields.names))
	dirty := false
	for _, f := range fields.names {
		if _, exists := m[f]; !exists {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: -2})
			continue
		}
		if _, ok := sidecar[f]; !ok {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: -1})
			continue
		}
		delete(sidecar, f)
		dirty = true
		out = append(out, protocol.Value{Kind: protocol.KindInteger, I: 1})
	}
	if dirty {
		if err := h.writeFieldExp(ctx, fields.key, sidecar); err != nil {
			return errValue(err)
		}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}
