package commands

import (
	"context"
	"path"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
)

func (s *stringHandler) registerKeyspace(r *network.Router) {
	r.Register("TYPE", s.type_)
	r.Register("EXISTS", s.exists)
	r.Register("KEYS", s.keys)
}

// typePrefixes 是全部类型前缀表；getAny 按表逐个探测，List/Set/ZSet
// 接入时在此追加前缀即可。
var typePrefixes = []string{"s:", "h:", "l:", "st:"}

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
