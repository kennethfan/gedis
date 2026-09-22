package commands

import (
	"context"
	"strconv"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (s *stringHandler) registerExtra(r *network.Router) {
	r.Register("APPEND", s.append)
	r.Register("STRLEN", s.strlen)
	r.Register("SETRANGE", s.setrange)
	r.Register("GETRANGE", s.getrange)
}

func (s *stringHandler) append(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'append' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	val, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid value")
	}
	var base []byte
	var expiry int64
	if e, err := s.getEntry(ctx, key); err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
	} else {
		base = e.Payload
		expiry = e.Expiry
	}
	out := make([]byte, 0, len(base)+len(val))
	out = append(out, base...)
	out = append(out, val...)
	if err := s.kv.Set(ctx, datastruct.StringKey(key), datastruct.EncodeString(out, expiry)); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(out))}
}

func (s *stringHandler) strlen(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'strlen' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	e, err := s.getEntry(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(e.Payload))}
}

func (s *stringHandler) setrange(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'setrange' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	offStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	offset, err := strconv.ParseInt(offStr, 10, 64)
	if err != nil || offset < 0 {
		return errValueStr("ERR offset is out of range")
	}
	val, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid value")
	}
	var base []byte
	var expiry int64
	if e, gerr := s.getEntry(ctx, key); gerr != nil {
		if !isNotFound(gerr) {
			return errValue(gerr)
		}
	} else {
		base = e.Payload
		expiry = e.Expiry
	}
	end := offset + int64(len(val))
	newLen := int64(len(base))
	if end > newLen {
		newLen = end
	}
	out := make([]byte, newLen)
	copy(out, base)
	copy(out[offset:], val)
	if err := s.kv.Set(ctx, datastruct.StringKey(key), datastruct.EncodeString(out, expiry)); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: newLen}
}

func (s *stringHandler) getrange(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'getrange' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	startStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	endStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	e, err := s.getEntry(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.BulkOf("")
		}
		return errValue(err)
	}
	n := int64(len(e.Payload))
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	if start > end || start >= n {
		return protocol.BulkOf("")
	}
	return protocol.Value{Kind: protocol.KindBulkString, Bulk: e.Payload[start : end+1]}
}
