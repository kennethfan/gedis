package commands

import (
	"context"
	"strconv"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// registerIncr 注册 INCR/DECR 系列，由 RegisterStrings 调用。
func (s *stringHandler) registerIncr(r *network.Router) {
	r.Register("INCR", s.incr)
	r.Register("INCRBY", s.incrby)
	r.Register("DECR", s.decr)
	r.Register("DECRBY", s.decrby)
	r.Register("INCRBYFLOAT", s.incrbyfloat)
}

func (s *stringHandler) incr(ctx context.Context, args []protocol.Value) protocol.Value {
	return s.incrBy(ctx, args, 1)
}

func (s *stringHandler) decr(ctx context.Context, args []protocol.Value) protocol.Value {
	return s.incrBy(ctx, args, -1)
}

func (s *stringHandler) incrby(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'incrby' command")
	}
	delta, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	n, err := strconv.ParseInt(delta, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	return s.incrBy(ctx, args[:1], n)
}

func (s *stringHandler) decrby(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'decrby' command")
	}
	delta, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	n, err := strconv.ParseInt(delta, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	return s.incrBy(ctx, args[:1], -n)
}

func (s *stringHandler) incrBy(ctx context.Context, args []protocol.Value, delta int64) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'incr' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getEntry(ctx, key)
	var cur int64
	var expiry int64
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
	} else {
		cur, err = strconv.ParseInt(string(e.Payload), 10, 64)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
		expiry = e.Expiry
	}
	if (delta > 0 && cur > (1<<63-1)-delta) || (delta < 0 && cur < (-1<<63)-delta) {
		return errValueStr("ERR increment or decrement would overflow")
	}
	next := cur + delta
	if err := s.kv.Set(ctx, datastruct.StringKey(key), datastruct.EncodeString([]byte(strconv.FormatInt(next, 10)), expiry)); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: next}
}

func (s *stringHandler) incrbyfloat(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'incrbyfloat' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	inc, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not a valid float")
	}
	delta, err := strconv.ParseFloat(inc, 64)
	if err != nil {
		return errValueStr("ERR value is not a valid float")
	}
	e, err := s.getEntry(ctx, key)
	var cur float64
	var expiry int64
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
	} else {
		cur, err = strconv.ParseFloat(string(e.Payload), 64)
		if err != nil {
			return errValueStr("ERR value is not a valid float")
		}
		expiry = e.Expiry
	}
	sum := cur + delta
	out := strconv.FormatFloat(sum, 'f', -1, 64)
	if err := s.kv.Set(ctx, datastruct.StringKey(key), datastruct.EncodeString([]byte(out), expiry)); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(out)}
}
