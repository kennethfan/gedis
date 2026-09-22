package commands

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
)

// KV 是命令层需要的最小存储接口，storage.Pebble 已满足。
type KV interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Scan(ctx context.Context, prefix []byte) ([][]byte, error)
	WriteBatch(ctx context.Context, ops []storage.BatchOp) error
}

// RegisterStrings 注册全部 string 与通用 key 命令。
func RegisterStrings(r *network.Router, kv KV) {
	h := &stringHandler{kv: kv}
	r.Register("SET", h.set)
	r.Register("GET", h.get)
	r.Register("GETDEL", h.getdel)
	r.Register("GETEX", h.getex)
	r.Register("DEL", h.del)
	r.Register("UNLINK", h.del)
	r.Register("TTL", h.ttl)
	h.registerMulti(r)
	h.registerIncr(r)
	h.registerExtra(r)
	h.registerKeyspace(r)
	h.registerExpire(r)
}

type stringHandler struct {
	kv KV
}

// getEntry 读 key 并解码：不存在返回 ErrNotFound；已过期则删除后返回
// ErrNotFound（被动过期）；类型非 string 返回 wrongType 错误。
func (s *stringHandler) getEntry(ctx context.Context, key string) (datastruct.Entry, error) {
	e, err := s.getAny(ctx, key)
	if err != nil {
		return datastruct.Entry{}, err
	}
	if e.Type != datastruct.TypeString {
		return datastruct.Entry{}, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return e, nil
}

func isNotFound(err error) bool {
	return err == storage.ErrNotFound
}

type setOptions struct {
	expiry  int64
	keepTTL bool
	nx      bool
	xx      bool
	get     bool
}

func parseSetArgs(args []protocol.Value) (value string, opt setOptions, errReply *protocol.Value) {
	if len(args) < 2 {
		return "", opt, errPtr("ERR wrong number of arguments for 'set' command")
	}
	v, ok := argString(args[1])
	if !ok {
		return "", opt, errPtr("ERR invalid value")
	}
	i := 2
	for i < len(args) {
		name, ok := argString(args[i])
		if !ok {
			return "", opt, errPtr("ERR syntax error")
		}
		switch strings.ToUpper(name) {
		case "NX":
			opt.nx = true
		case "XX":
			opt.xx = true
		case "KEEPTTL":
			opt.keepTTL = true
		case "GET":
			opt.get = true
		case "EX", "PX", "EXAT", "PXAT":
			i++
			if i >= len(args) {
				return "", opt, errPtr("ERR syntax error")
			}
			n, ok := argString(args[i])
			if !ok {
				return "", opt, errPtr("ERR value is not an integer or out of range")
			}
			exp, err := parseExpiry(name, n)
			if err != nil {
				return "", opt, errPtr(err.Error())
			}
			opt.expiry = exp
		default:
			return "", opt, errPtr("ERR syntax error")
		}
		i++
	}
	if opt.nx && opt.xx {
		return "", opt, errPtr("ERR syntax error")
	}
	return v, opt, nil
}

func parseExpiry(kind, s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}
	var exp int64
	switch kind {
	case "EX":
		if n <= 0 {
			return 0, fmt.Errorf("ERR invalid expire time in 'set' command")
		}
		exp = time.Now().Add(time.Duration(n) * time.Second).UnixNano()
	case "PX":
		if n <= 0 {
			return 0, fmt.Errorf("ERR invalid expire time in 'set' command")
		}
		exp = time.Now().Add(time.Duration(n) * time.Millisecond).UnixNano()
	case "EXAT":
		exp = time.Unix(n, 0).UnixNano()
	case "PXAT":
		exp = time.Unix(0, n*int64(time.Millisecond)).UnixNano()
	}
	return exp, nil
}

func (s *stringHandler) set(ctx context.Context, args []protocol.Value) protocol.Value {
	value, opt, errReply := parseSetArgs(args)
	if errReply != nil {
		return *errReply
	}
	key, _ := argString(args[0])

	old, err := s.getEntry(ctx, key)
	exists := err == nil
	if err != nil && !isNotFound(err) {
		return errValue(err)
	}
	if opt.nx && exists {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	if opt.xx && !exists {
		return protocol.Value{Kind: protocol.KindBulkString}
	}

	expiry := opt.expiry
	if opt.keepTTL && exists {
		expiry = old.Expiry
	}
	if err := s.kv.Set(ctx, datastruct.StringKey(key), datastruct.EncodeString([]byte(value), expiry)); err != nil {
		return errValue(err)
	}
	if opt.get {
		if !exists {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return protocol.Value{Kind: protocol.KindBulkString, Bulk: old.Payload}
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (s *stringHandler) get(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'get' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getEntry(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindBulkString, Bulk: e.Payload}
}

func (s *stringHandler) getdel(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'getdel' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getEntry(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	_ = s.kv.Delete(ctx, datastruct.StringKey(key))
	return protocol.Value{Kind: protocol.KindBulkString, Bulk: e.Payload}
}

func (s *stringHandler) getex(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 3 {
		return errValueStr("ERR wrong number of arguments for 'getex' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getEntry(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	expiry := e.Expiry
	if len(args) == 3 {
		name, ok := argString(args[1])
		n, ok2 := argString(args[2])
		if !ok || !ok2 {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(name) {
		case "PERSIST":
			_ = n
			expiry = 0
		case "EX", "PX", "EXAT", "PXAT":
			exp, perr := parseExpiry(strings.ToUpper(name), n)
			if perr != nil {
				return errValue(perr)
			}
			expiry = exp
		default:
			return errValueStr("ERR syntax error")
		}
	} else if len(args) == 2 {
		return errValueStr("ERR syntax error")
	}
	if expiry != e.Expiry {
		if err := s.kv.Set(ctx, datastruct.StringKey(key), datastruct.EncodeString(e.Payload, expiry)); err != nil {
			return errValue(err)
		}
	}
	return protocol.Value{Kind: protocol.KindBulkString, Bulk: e.Payload}
}

func (s *stringHandler) del(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'del' command")
	}
	var n int64
	for _, a := range args {
		key, ok := argString(a)
		if !ok {
			continue
		}
		storeKey, e, err := lookupRaw(ctx, s.kv, key)
		if err != nil {
			continue
		}
		_ = s.kv.Delete(ctx, storeKey)
		if e.Type == datastruct.TypeHash {
			_ = s.kv.Delete(ctx, datastruct.HashExpKey(key))
		}
		n++
	}
	return protocol.Value{Kind: protocol.KindInteger, I: n}
}

func (s *stringHandler) ttl(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'ttl' command")
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
	return protocol.Value{Kind: protocol.KindInteger, I: (ms + 500) / 1000}
}

func argString(v protocol.Value) (string, bool) {
	if v.Kind != protocol.KindBulkString {
		return "", false
	}
	out := make([]byte, len(v.Bulk))
	copy(out, v.Bulk)
	return string(out), true
}

func errValue(err error) protocol.Value {
	return protocol.Value{Kind: protocol.KindError, S: err.Error()}
}

func errValueStr(msg string) protocol.Value {
	return protocol.Value{Kind: protocol.KindError, S: msg}
}

func errPtr(msg string) *protocol.Value {
	v := errValueStr(msg)
	return &v
}
