package commands

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *streamHandler) registerClaim(r *network.Router) {
	r.Register("XACK", h.xack)
	r.Register("XPENDING", h.xpending)
}

const xclaimNogroupFmt = "NOGROUP No such key '%s' or consumer group '%s'"

// loadGroup 取 stream + 组：返回 ok=false 时 errVal 为 NOGROUP（缺 key/组）
// 或 WRONGTYPE，missing 区分两者。
func (h *streamHandler) loadGroup(ctx context.Context, key, group string) (*datastruct.Stream, int64, *datastruct.StreamGroup, protocol.Value, bool, bool) {
	s, expiry, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return nil, 0, nil, errValueStr(fmt.Sprintf(xclaimNogroupFmt, key, group)), false, true
		}
		return nil, 0, nil, errValue(err), false, false
	}
	g := findGroup(s, group)
	if g == nil {
		return nil, 0, nil, errValueStr(fmt.Sprintf(xclaimNogroupFmt, key, group)), false, true
	}
	return s, expiry, g, protocol.Value{}, true, false
}

// xack 真实现：从 PEL 删除给定 IDs，返回删除数；缺 key/组返 0。
func (h *streamHandler) xack(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'xack' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	ids := make([]datastruct.StreamID, 0, len(args)-2)
	for _, a := range args[2:] {
		s, ok := argString(a)
		if !ok {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
		id, auto, err := datastruct.StreamParseID(s)
		if err != nil || auto {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
		ids = append(ids, id)
	}
	s, expiry, g, errVal, ok, missing := h.loadGroup(ctx, key, groupName)
	if !ok {
		if missing {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errVal
	}
	var n int64
	for _, id := range ids {
		if g.DelPEL(id) {
			n++
		}
	}
	if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: n}
}

func (h *streamHandler) xpending(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'xpending' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	_, _, g, errVal, ok, _ := h.loadGroup(ctx, key, groupName)
	if !ok {
		return errVal
	}
	if len(args) == 2 {
		return pendingSummary(g)
	}
	rest := args[2:]
	var idleMs *int64
	if len(rest) > 0 {
		if opt, ok := argString(rest[0]); ok && strings.EqualFold(opt, "IDLE") {
			if len(rest) < 2 {
				return errValueStr("ERR syntax error")
			}
			n, ok := parseIntArg(rest[1])
			if !ok {
				return errValueStr("ERR value is not an integer or out of range")
			}
			idleMs = &n
			rest = rest[2:]
		}
	}
	if len(rest) < 3 {
		return errValueStr("ERR syntax error")
	}
	// 多余参数截断式忽略（对标真 Redis：consumer 位取第 4 个）。
	if len(rest) > 4 {
		rest = rest[:4]
	}
	startStr, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR Invalid stream ID specified as stream command argument")
	}
	endStr, ok := argString(rest[1])
	if !ok {
		return errValueStr("ERR Invalid stream ID specified as stream command argument")
	}
	start, startExcl, err := parsePelBound(startStr)
	if err != nil {
		return errValue(err)
	}
	end, endExcl, err := parsePelBound(endStr)
	if err != nil {
		return errValue(err)
	}
	count, ok := parseIntArg(rest[2])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	if count <= 0 {
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	}
	var consumer *string
	if len(rest) == 4 {
		c, ok := argString(rest[3])
		if !ok {
			return errValueStr("ERR invalid consumer")
		}
		consumer = &c
	}
	nowMs := uint64(time.Now().UnixMilli())
	out := make([]protocol.Value, 0)
	for _, p := range g.PEL {
		if consumer != nil && p.Consumer != *consumer {
			continue
		}
		if cmp := p.ID.Compare(start); cmp < 0 || (cmp == 0 && startExcl) {
			continue
		}
		if cmp := p.ID.Compare(end); cmp > 0 || (cmp == 0 && endExcl) {
			continue
		}
		idle := int64(0)
		if nowMs > p.DeliveryMs {
			idle = int64(nowMs - p.DeliveryMs)
		}
		if idleMs != nil && idle < *idleMs {
			continue
		}
		if int64(len(out)) >= count {
			break
		}
		out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf(p.ID.String()),
			protocol.BulkOf(p.Consumer),
			protocol.Value{Kind: protocol.KindInteger, I: idle},
			protocol.Value{Kind: protocol.KindInteger, I: int64(p.Count)},
		}})
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

// parsePelBound 解析 PEL 区间端点："-"/"+" 极值，"(" 前缀排他，其余严格 ID。
func parsePelBound(s string) (datastruct.StreamID, bool, error) {
	if s == "-" {
		return datastruct.StreamMinID, false, nil
	}
	if s == "+" {
		return datastruct.StreamMaxID, false, nil
	}
	excl := false
	if strings.HasPrefix(s, "(") {
		excl = true
		s = s[1:]
	}
	id, auto, err := datastruct.StreamParseID(s)
	if err != nil || auto {
		return datastruct.StreamID{}, false, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	return id, excl, nil
}

// pendingSummary 生成 [count, min-or-nil, max-or-nil, [[owner, n]…]]（owner 按名排序）。
func pendingSummary(g *datastruct.StreamGroup) protocol.Value {
	minV, maxV := nilBulk, nilBulk
	if len(g.PEL) > 0 {
		minV = protocol.BulkOf(g.PEL[0].ID.String())
		maxV = protocol.BulkOf(g.PEL[len(g.PEL)-1].ID.String())
	}
	counts := map[string]int64{}
	for _, p := range g.PEL {
		counts[p.Consumer]++
	}
	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	owners := make([]protocol.Value, 0, len(names))
	for _, n := range names {
		owners = append(owners, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf(n),
			protocol.Value{Kind: protocol.KindInteger, I: counts[n]},
		}})
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.Value{Kind: protocol.KindInteger, I: int64(len(g.PEL))},
		minV, maxV,
		protocol.Value{Kind: protocol.KindArray, Elems: owners},
	}}
}
