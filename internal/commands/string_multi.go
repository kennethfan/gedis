package commands

import (
	"context"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
)

// registerMulti 注册 MSET/MGET/MSETNX，由 RegisterStrings 调用。
func (s *stringHandler) registerMulti(r *network.Router) {
	r.Register("MSET", s.mset)
	r.Register("MGET", s.mget)
	r.Register("MSETNX", s.msetnx)
}

func (s *stringHandler) mset(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return errValueStr("ERR wrong number of arguments for 'mset' command")
	}
	ops := make([]storage.BatchOp, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR invalid key")
		}
		val, ok := argString(args[i+1])
		if !ok {
			return errValueStr("ERR invalid value")
		}
		ops = append(ops, storage.BatchOp{
			Key:   datastruct.StringKey(key),
			Value: datastruct.EncodeString([]byte(val), 0),
		})
	}
	if err := s.kv.WriteBatch(ctx, ops); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (s *stringHandler) mget(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'mget' command")
	}
	out := make([]protocol.Value, len(args))
	for i, a := range args {
		key, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid key")
		}
		e, err := s.getEntry(ctx, key)
		if err != nil {
			if isNotFound(err) {
				out[i] = protocol.Value{Kind: protocol.KindNull}
				continue
			}
			return errValue(err)
		}
		out[i] = protocol.Value{Kind: protocol.KindBulkString, Bulk: e.Payload}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (s *stringHandler) msetnx(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return errValueStr("ERR wrong number of arguments for 'msetnx' command")
	}
	for i := 0; i < len(args); i += 2 {
		key, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR invalid key")
		}
		if _, err := s.getEntry(ctx, key); err == nil {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		} else if !isNotFound(err) {
			return errValue(err)
		}
	}
	for i := 0; i < len(args); i += 2 {
		key, _ := argString(args[i])
		val, _ := argString(args[i+1])
		if err := s.kv.Set(ctx, datastruct.StringKey(key), datastruct.EncodeString([]byte(val), 0)); err != nil {
			return errValue(err)
		}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 1}
}
