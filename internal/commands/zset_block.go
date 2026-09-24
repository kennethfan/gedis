package commands

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *zsetHandler) registerBlockZ(r *network.Router) {
	r.Register("BZPOPMIN", h.bzpopmin)
	r.Register("BZPOPMAX", h.bzpopmax)
	r.Register("BZMPOP", h.bzmpop)
}

// tryZPopSingle 从单个 key 弹 count 个元素：rev=false 取最小端。
// 缺失 key 返回 found=false；空 key 删 key。
func (h *zsetHandler) tryZPopSingle(ctx context.Context, key string, count int64, rev bool) (popped []zsetMember, found bool, err error) {
	z, expiry, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(z) == 0 {
		return nil, false, nil
	}
	sorted := sortedZSet(z)
	if rev {
		for i, j := 0, len(sorted)-1; i < j; i, j = i+1, j-1 {
			sorted[i], sorted[j] = sorted[j], sorted[i]
		}
	}
	if count > int64(len(sorted)) {
		count = int64(len(sorted))
	}
	popped = sorted[:count]
	for _, p := range popped {
		delete(z, p.m)
	}
	if werr := h.writeZSet(ctx, key, z, expiry); werr != nil {
		return nil, false, werr
	}
	return popped, true, nil
}

// blockZPop 轮询 keys 直到弹出、超时或 ctx 取消；timeout<=0 表无限等待。
func (h *zsetHandler) blockZPop(ctx context.Context, keys []string, count int64, rev bool, timeout time.Duration) (key string, popped []zsetMember, ok bool) {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		for _, k := range keys {
			p, found, err := h.tryZPopSingle(ctx, k, count, rev)
			if err != nil {
				return "", nil, false
			}
			if found {
				return k, p, true
			}
		}
		if timeout > 0 && !time.Now().Before(deadline) {
			return "", nil, false
		}
		select {
		case <-ctx.Done():
			return "", nil, false
		case <-time.After(blockPollInterval):
		}
	}
}

func zpopReply(key string, popped []zsetMember) protocol.Value {
	out := []protocol.Value{protocol.BulkOf(key)}
	for _, p := range popped {
		out = append(out, protocol.BulkOf(p.m), protocol.BulkOf(formatScore(p.s)))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *zsetHandler) bzpopmin(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.bzpop(ctx, args, false, "bzpopmin")
}

func (h *zsetHandler) bzpopmax(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.bzpop(ctx, args, true, "bzpopmax")
}

func (h *zsetHandler) bzpop(ctx context.Context, args []protocol.Value, rev bool, name string) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for '" + name + "' command")
	}
	timeoutStr, ok := argString(args[len(args)-1])
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	timeout, ok := parseTimeout(timeoutStr)
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[:len(args)-1] {
		k, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid key")
		}
		keys = append(keys, k)
	}
	for _, k := range keys {
		if _, _, err := h.readZSet(ctx, k); err != nil && !isNotFound(err) {
			return errValue(err)
		}
	}
	k, popped, ok := h.blockZPop(ctx, keys, 1, rev, timeout)
	if !ok {
		return protocol.Value{Kind: protocol.KindArray}
	}
	return zpopReply(k, popped)
}

// bzmpop 实现 BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n]。
func (h *zsetHandler) bzmpop(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 4 {
		return errValueStr("ERR wrong number of arguments for 'bzmpop' command")
	}
	timeoutStr, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	timeout, ok := parseTimeout(timeoutStr)
	if !ok {
		return errValueStr("ERR timeout is not a float or out of range")
	}
	numkeysStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	numkeys, err := strconv.ParseInt(numkeysStr, 10, 64)
	if err != nil || numkeys <= 0 || int(numkeys) > len(args)-2 {
		return errValueStr("ERR value is not an integer or out of range")
	}
	keys := make([]string, 0, numkeys)
	for _, a := range args[2 : 2+numkeys] {
		k, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid key")
		}
		keys = append(keys, k)
	}
	rest := args[2+numkeys:]
	if len(rest) < 1 || len(rest) > 3 {
		return errValueStr("ERR syntax error")
	}
	dirStr, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	var rev bool
	switch strings.ToUpper(dirStr) {
	case "MIN":
		rev = false
	case "MAX":
		rev = true
	default:
		return errValueStr("ERR syntax error")
	}
	var count int64 = 1
	if len(rest) == 3 {
		where, ok := argString(rest[1])
		if !ok || !strings.EqualFold(where, "COUNT") {
			return errValueStr("ERR syntax error")
		}
		cstr, ok := argString(rest[2])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		count, ok = parseCount(cstr)
		if !ok || count < 1 {
			return errValueStr("ERR value is out of range, must be positive")
		}
	} else if len(rest) == 2 {
		return errValueStr("ERR syntax error")
	}
	for _, k := range keys {
		if _, _, err := h.readZSet(ctx, k); err != nil && !isNotFound(err) {
			return errValue(err)
		}
	}
	k, popped, ok := h.blockZPop(ctx, keys, count, rev, timeout)
	if !ok {
		return protocol.Value{Kind: protocol.KindArray}
	}
	pairs := make([]protocol.Value, 0, len(popped))
	for _, p := range popped {
		pairs = append(pairs, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf(p.m), protocol.BulkOf(formatScore(p.s)),
		}})
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(k),
		{Kind: protocol.KindArray, Elems: pairs},
	}}
}
