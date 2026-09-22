package commands

import (
	"context"
	"strconv"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *hashHandler) registerIncr(r *network.Router) {
	r.Register("HINCRBY", h.hincrby)
	r.Register("HINCRBYFLOAT", h.hincrbyfloat)
}

func (h *hashHandler) hincrby(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'hincrby' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	field, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid field")
	}
	deltaStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	delta, err := strconv.ParseInt(deltaStr, 10, 64)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	m, expiry, rerr := h.readHash(ctx, key)
	if rerr != nil {
		if !isNotFound(rerr) {
			return errValue(rerr)
		}
		m = make(map[string]string)
	}
	var cur int64
	if v, exists := m[field]; exists {
		cur, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return errValueStr("ERR hash value is not an integer")
		}
	}
	if (delta > 0 && cur > (1<<63-1)-delta) || (delta < 0 && cur < (-1<<63)-delta) {
		return errValueStr("ERR increment or decrement would overflow")
	}
	next := cur + delta
	m[field] = strconv.FormatInt(next, 10)
	if err := h.writeHash(ctx, key, m, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: next}
}

func (h *hashHandler) hincrbyfloat(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'hincrbyfloat' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	field, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid field")
	}
	incStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR value is not a valid float")
	}
	delta, err := strconv.ParseFloat(incStr, 64)
	if err != nil {
		return errValueStr("ERR value is not a valid float")
	}
	m, expiry, rerr := h.readHash(ctx, key)
	if rerr != nil {
		if !isNotFound(rerr) {
			return errValue(rerr)
		}
		m = make(map[string]string)
	}
	var cur float64
	if v, exists := m[field]; exists {
		cur, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return errValueStr("ERR hash value is not a valid float")
		}
	}
	out := strconv.FormatFloat(cur+delta, 'f', -1, 64)
	m[field] = out
	if err := h.writeHash(ctx, key, m, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(out)}
}
