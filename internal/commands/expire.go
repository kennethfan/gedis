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

var expireMeta = []acl.Meta{
	{Name: "EXPIRE", Category: "keyspace", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "PEXPIRE", Category: "keyspace", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "EXPIREAT", Category: "keyspace", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "PEXPIREAT", Category: "keyspace", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "PERSIST", Category: "keyspace", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "PTTL", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "EXPIRETIME", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "PEXPIRETIME", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "OBJECT", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{Custom: acl.SubKeyAt1}},
}

func (s *stringHandler) registerExpire(r *network.Router) {
	for _, m := range expireMeta {
		acl.RegisterMeta(m)
	}
	r.Register("EXPIRE", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return s.expire(ctx, args, expireRelativeSec)
	})
	r.Register("PEXPIRE", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return s.expire(ctx, args, expireRelativeMs)
	})
	r.Register("EXPIREAT", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return s.expire(ctx, args, expireAbsoluteSec)
	})
	r.Register("PEXPIREAT", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return s.expire(ctx, args, expireAbsoluteMs)
	})
	r.Register("PERSIST", s.persist)
	r.Register("PTTL", s.pttl)
	r.Register("EXPIRETIME", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return s.expireTime(ctx, args, false)
	})
	r.Register("PEXPIRETIME", func(ctx context.Context, args []protocol.Value) protocol.Value {
		return s.expireTime(ctx, args, true)
	})
	r.Register("OBJECT", s.object)
}

type expireKind int

const (
	expireRelativeSec expireKind = iota
	expireRelativeMs
	expireAbsoluteSec
	expireAbsoluteMs
)

type expireCond int

const (
	condNone expireCond = iota
	condNX
	condXX
	condGT
	condLT
)

func (s *stringHandler) expire(ctx context.Context, args []protocol.Value, kind expireKind) protocol.Value {
	if len(args) < 2 || len(args) > 3 {
		return errValueStr("ERR wrong number of arguments for 'expire' command")
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
	cond := condNone
	if len(args) == 3 {
		name, ok := argString(args[2])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(name) {
		case "NX":
			cond = condNX
		case "XX":
			cond = condXX
		case "GT":
			cond = condGT
		case "LT":
			cond = condLT
		default:
			return errValueStr("ERR syntax error")
		}
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

	e, gerr := s.getAny(ctx, key)
	if gerr != nil {
		if isNotFound(gerr) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(gerr)
	}
	storeKey, _, rerr := lookupRaw(ctx, s.kv, key)
	if rerr != nil {
		return errValue(rerr)
	}

	if exp <= time.Now().UnixNano() {
		_ = s.kv.Delete(ctx, storeKey)
		return protocol.Value{Kind: protocol.KindInteger, I: 1}
	}

	switch cond {
	case condNX:
		if e.Expiry != 0 {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
	case condXX:
		if e.Expiry == 0 {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
	case condGT:
		if e.Expiry != 0 && exp <= e.Expiry {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
	case condLT:
		if e.Expiry == 0 || exp >= e.Expiry {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
	}

	if err := s.kv.Set(ctx, storeKey, datastruct.Encode(e.Type, exp, e.Payload)); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 1}
}

func (s *stringHandler) persist(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'persist' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getAny(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	if e.Expiry == 0 {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	storeKey, _, rerr := lookupRaw(ctx, s.kv, key)
	if rerr != nil {
		return errValue(rerr)
	}
	if serr := s.kv.Set(ctx, storeKey, datastruct.Encode(e.Type, 0, e.Payload)); serr != nil {
		return errValue(serr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 1}
}

func (s *stringHandler) pttl(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'pttl' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getAny(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: -2}
		}
		return errValue(err)
	}
	if e.Expiry == 0 {
		return protocol.Value{Kind: protocol.KindInteger, I: -1}
	}
	ms := (e.Expiry - time.Now().UnixNano()) / int64(time.Millisecond)
	if ms < 0 {
		ms = 0
	}
	return protocol.Value{Kind: protocol.KindInteger, I: ms}
}

// expireTime 实现 EXPIRETIME/PEXPIRETIME：回绝对 Unix 时间（秒/毫秒）；
// miss -2、无过期 -1（对齐真机与 pttl 语义）。
func (s *stringHandler) expireTime(ctx context.Context, args []protocol.Value, ms bool) protocol.Value {
	name := "expiretime"
	if ms {
		name = "pexpiretime"
	}
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for '" + name + "' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getAny(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: -2}
		}
		return errValue(err)
	}
	if e.Expiry == 0 {
		return protocol.Value{Kind: protocol.KindInteger, I: -1}
	}
	if ms {
		return protocol.Value{Kind: protocol.KindInteger, I: e.Expiry / int64(time.Millisecond)}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: e.Expiry / int64(time.Second)}
}

func (s *stringHandler) object(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'object' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	// REFCOUNT/IDLETIME/FREQ：本引擎无引用计数与访问时钟跟踪，
	// 回恒定占位（1/0/0）并文档注明；LFU 真值等 Phase 9 补齐。
	switch strings.ToUpper(sub) {
	case "REFCOUNT", "IDLETIME", "FREQ":
		key, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR invalid key")
		}
		if _, err := s.getAny(ctx, key); err != nil {
			if isNotFound(err) {
				return protocol.Value{Kind: protocol.KindBulkString}
			}
			return errValue(err)
		}
		if strings.EqualFold(sub, "REFCOUNT") {
			return protocol.Value{Kind: protocol.KindInteger, I: 1}
		}
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	case "ENCODING":
		break
	default:
		return errValueStr("ERR syntax error")
	}
	key, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getAny(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	enc, ok := objectEncodingOf(e)
	if !ok {
		return errValueStr("ERR OBJECT ENCODING not supported for this type")
	}
	return protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(enc)}
}

// objectEncodingOf 返回 entry 的编码名；未覆盖类型回 ok=false。
func objectEncodingOf(e datastruct.Entry) (string, bool) {
	switch e.Type {
	case datastruct.TypeHash:
		if len(e.Payload) > 0 && e.Payload[0] == datastruct.EncodingHashtable {
			return "hashtable", true
		}
		return "listpack", true
	case datastruct.TypeList:
		if len(e.Payload) > 0 && e.Payload[0] == datastruct.ListEncodingQuicklist {
			return "quicklist", true
		}
		return "ziplist", true
	case datastruct.TypeSet:
		if len(e.Payload) > 0 && e.Payload[0] == datastruct.EncodingIntset {
			return "intset", true
		}
		return "hashtable", true
	case datastruct.TypeZSet:
		if len(e.Payload) > 0 && e.Payload[0] == datastruct.ZSetEncodingSkiplist {
			return "skiplist", true
		}
		return "listpack", true
	case datastruct.TypeString:
		if len(e.Payload) <= 44 {
			return "embstr", true
		}
		return "raw", true
	default:
		return "", false
	}
}
