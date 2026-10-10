package commands

import (
	"context"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
)

var keyspaceMeta = []acl.Meta{
	{Name: "TYPE", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "EXISTS", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "KEYS", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{First: -1}},
	{Name: "MOVE", Category: "keyspace", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "RANDOMKEY", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{First: -1}},
}

func (s *stringHandler) registerKeyspace(r *network.Router) {
	for _, m := range keyspaceMeta {
		acl.RegisterMeta(m)
	}
	r.Register("TYPE", s.type_)
	r.Register("EXISTS", s.exists)
	r.Register("KEYS", s.keys)
	r.Register("MOVE", s.move)
	r.Register("RANDOMKEY", s.randomKey)
}

// typePrefixes 是全部类型前缀表；getAny 按表逐个探测，List/Set/ZSet
// 接入时在此追加前缀即可。
var typePrefixes = []string{"s:", "h:", "l:", "st:", "z:", "hll:", "x:"}

func (s *stringHandler) getAny(ctx context.Context, key string) (datastruct.Entry, error) {
	return lookupKey(ctx, s.kv, key)
}

func lookupKey(ctx context.Context, kv KV, key string) (datastruct.Entry, error) {
	_, e, err := lookupRaw(ctx, kv, key)
	return e, err
}

// lookupRaw 与 lookupKey 同构，额外返回命中的实际存储 key（带类型前缀）。
// expire/persist 回写必须用它，不能假设 key 是 string 类型。
func lookupRaw(ctx context.Context, kv KV, key string) ([]byte, datastruct.Entry, error) {
	for _, p := range typePrefixes {
		raw := []byte(p + key)
		val, err := kv.Get(ctx, raw)
		if err != nil {
			if !isNotFound(err) {
				return nil, datastruct.Entry{}, err
			}
			continue
		}
		e, err := datastruct.Decode(val)
		if err != nil {
			return nil, datastruct.Entry{}, err
		}
		if e.Expiry != 0 && time.Now().UnixNano() >= e.Expiry {
			_ = kv.Delete(ctx, raw)
			_ = kv.Delete(ctx, datastruct.HashExpKey(key))
			network.StatsFromContext(ctx).IncExpired()
			return nil, datastruct.Entry{}, storage.ErrNotFound
		}
		return raw, e, nil
	}
	return nil, datastruct.Entry{}, storage.ErrNotFound
}

func typeName(t byte) string {
	switch t {
	case datastruct.TypeString:
		return "string"
	case datastruct.TypeHash:
		return "hash"
	case datastruct.TypeList:
		return "list"
	case datastruct.TypeSet:
		return "set"
	case datastruct.TypeZSet:
		return "zset"
	case datastruct.TypeHLL:
		return "string"
	case datastruct.TypeStream:
		return "stream"
	default:
		return "none"
	}
}

func (s *stringHandler) type_(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'type' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getAny(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindSimpleString, S: "none"}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: typeName(e.Type)}
}

func (s *stringHandler) exists(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'exists' command")
	}
	var n int64
	for _, a := range args {
		key, ok := argString(a)
		if !ok {
			continue
		}
		if _, err := s.getAny(ctx, key); err != nil {
			if !isNotFound(err) {
				return errValue(err)
			}
			continue
		}
		n++
	}
	return protocol.Value{Kind: protocol.KindInteger, I: n}
}

// stripTypePrefix 去掉最长匹配的类型前缀；"s:" 是 "st:" 的字面前缀，
// 必须按最长匹配剥离，否则 Scan("s:") 会把 st: key 算进来。
func stripTypePrefix(k string) (string, bool) {
	best := ""
	for _, p := range typePrefixes {
		if strings.HasPrefix(k, p) && len(p) > len(best) {
			best = p
		}
	}
	if best == "" {
		return "", false
	}
	return strings.TrimPrefix(k, best), true
}

// allUserKeys 按前缀表顺序返回去重后的用户 key（名 + 存储 key）。
func allUserKeys(ctx context.Context, kv KV) ([]string, [][]byte, error) {
	var names []string
	var raws [][]byte
	seen := make(map[string]struct{})
	for _, p := range typePrefixes {
		raw, err := kv.Scan(ctx, []byte(p))
		if err != nil {
			return nil, nil, err
		}
		for _, k := range raw {
			name, ok := stripTypePrefix(string(k))
			if !ok {
				continue
			}
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
			raws = append(raws, k)
		}
	}
	return names, raws, nil
}

func (s *stringHandler) keys(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'keys' command")
	}
	pattern, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid pattern")
	}
	names, _, err := allUserKeys(ctx, s.kv)
	if err != nil {
		return errValue(err)
	}
	out := make([]protocol.Value, 0, len(names))
	for _, name := range names {
		matched, merr := path.Match(pattern, name)
		if merr != nil || !matched {
			continue
		}
		out = append(out, protocol.BulkOf(name))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

// move 实现 MOVE：单库引擎，同库回源/目标相同错，异库恒回 0
// （无处可搬；miss 同样 0，对齐真机 miss 语义）。
func (s *stringHandler) move(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'move' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	dbStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	db, err := strconv.Atoi(dbStr)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	if db < 0 {
		return errValueStr("ERR DB index is out of range")
	}
	if db == 0 {
		return errValueStr("ERR source and destination objects are the same")
	}
	if _, err := s.getAny(ctx, key); err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	// TODO(真机对齐): 单库实现恒不移库；多库支持落地时在此成功分支补
	// Notify("g","move_from",key)+Notify("g","move_to",dst)（事件表要求）。
	return protocol.Value{Kind: protocol.KindInteger, I: 0}
}

func (s *stringHandler) randomKey(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'randomkey' command")
	}
	names, _, err := allUserKeys(ctx, s.kv)
	if err != nil {
		return errValue(err)
	}
	if len(names) == 0 {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	return protocol.BulkOf(names[rand.Intn(len(names))])
}
