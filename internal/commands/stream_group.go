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
)

func (h *streamHandler) registerGroup(r *network.Router) {
	r.Register("XREADGROUP", h.xreadgroup)
}

const xreadgroupNogroupFmt = "NOGROUP No such key '%s' or consumer group '%s' in XREADGROUP with GROUP option"

type readgroupReq struct {
	key      string
	isNew    bool
	explicit datastruct.StreamID
}

// xreadgroup 实现 XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms]
// [NOACK] STREAMS key… id…：" >" 取 LastID 之后新消息并建 PEL（NOACK 跳过建 PEL
// 但仍推进 last/read）；显式 ID 仅重投属主==读者的 PEL entries（count+1）。
func (h *streamHandler) xreadgroup(ctx context.Context, args []protocol.Value) protocol.Value {
	// 最短合法形 GROUP g c STREAMS k id（6 参数，对标真 Redis 先验 arity）。
	if len(args) < 6 {
		return errValueStr("ERR wrong number of arguments for 'xreadgroup' command")
	}
	kw, ok := argString(args[0])
	if !ok || !strings.EqualFold(kw, "GROUP") {
		return errValueStr("ERR wrong number of arguments for 'xreadgroup' command")
	}
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	consumerName, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid consumer")
	}
	count := int64(0)
	var blockMs *int64
	noack := false
	i := 3
	for ; i < len(args); i++ {
		opt, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		if strings.EqualFold(opt, "STREAMS") {
			break
		}
		switch {
		case strings.EqualFold(opt, "COUNT"):
			i++
			if i >= len(args) {
				return errValueStr("ERR syntax error")
			}
			n, ok := parseIntArg(args[i])
			if !ok {
				return errValueStr("ERR value is not an integer or out of range")
			}
			count = n
		case strings.EqualFold(opt, "BLOCK"):
			i++
			if i >= len(args) {
				return errValueStr("ERR syntax error")
			}
			n, ok := parseIntArg(args[i])
			if !ok {
				return errValueStr("ERR timeout is not an integer or out of range")
			}
			if n < 0 {
				return errValueStr("ERR timeout is negative")
			}
			blockMs = &n
		case strings.EqualFold(opt, "NOACK"):
			noack = true
		default:
			return errValueStr("ERR syntax error")
		}
	}
	if i >= len(args) {
		return errValueStr("ERR wrong number of arguments for 'xreadgroup' command")
	}
	rest := args[i+1:]
	if len(rest) < 2 {
		return errValueStr("ERR wrong number of arguments for 'xreadgroup' command")
	}
	if len(rest)%2 != 0 {
		return errValueStr("ERR Unbalanced 'xreadgroup' command")
	}
	nk := len(rest) / 2
	reqs := make([]readgroupReq, 0, nk)
	for k := 0; k < nk; k++ {
		key, ok := argString(rest[k])
		if !ok {
			return errValueStr("ERR invalid key")
		}
		idStr, ok := argString(rest[k+nk])
		if !ok {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
		if idStr == ">" {
			reqs = append(reqs, readgroupReq{key: key, isNew: true})
			continue
		}
		if idStr == "$" {
			return errValueStr("ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.")
		}
		id, auto, err := datastruct.StreamParseID(idStr)
		if err != nil || auto {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
		reqs = append(reqs, readgroupReq{key: key, explicit: id})
	}
	nowMs := uint64(time.Now().UnixMilli())
	// 先校验全部 key+组：缺失报 NOGROUP，类型错报 WRONGTYPE。
	rs := make([]resolvedGroup, 0, len(reqs))
	for _, q := range reqs {
		s, expiry, err := h.readStream(ctx, q.key)
		if err != nil {
			if isNotFound(err) {
				return errValueStr(fmt.Sprintf(xreadgroupNogroupFmt, q.key, groupName))
			}
			return errValue(err)
		}
		g := findGroup(s, groupName)
		if g == nil {
			return errValueStr(fmt.Sprintf(xreadgroupNogroupFmt, q.key, groupName))
		}
		rs = append(rs, resolvedGroup{req: q, s: s, expiry: expiry, g: g})
	}
	// BLOCK 轮询直到任一 key 有数据、超时或取消。
	if blockMs != nil {
		var deadline time.Time
		if *blockMs > 0 {
			deadline = time.Now().Add(time.Duration(*blockMs) * time.Millisecond)
		}
		blocked := false
		defer func() {
			if blocked {
				h.stats.DecBlocked()
			}
		}()
		for !groupHasData(rs, consumerName) {
			if blockMs != nil && *blockMs > 0 && !time.Now().Before(deadline) {
				return protocol.Value{Kind: protocol.KindArray}
			}
			if !blocked {
				h.stats.IncBlocked()
				blocked = true
			}
			select {
			case <-ctx.Done():
				return protocol.Value{Kind: protocol.KindArray}
			case <-time.After(blockPollInterval):
			}
			rs = rs[:0]
			for _, q := range reqs {
				s, expiry, err := h.readStream(ctx, q.key)
				if err != nil {
					return protocol.Value{Kind: protocol.KindArray}
				}
				g := findGroup(s, groupName)
				if g == nil {
					return protocol.Value{Kind: protocol.KindArray}
				}
				rs = append(rs, resolvedGroup{req: q, s: s, expiry: expiry, g: g})
			}
		}
	}
	var out []protocol.Value
	for _, r := range rs {
		c := &r.g.Consumers[r.g.EnsureConsumer(consumerName, nowMs)]
		c.SeenMs = nowMs
		var vals []protocol.Value
		if r.req.isNew {
			vals = h.deliverNew(r.s, r.g, c, count, noack, nowMs)
			n := int64(len(vals))
			if n > 0 {
				r.g.EntriesRead += uint64(n)
				r.g.HasRead = true
			}
		} else {
			vals = h.redeliver(r.s, r.g, c, r.req.explicit, count, nowMs)
		}
		if len(vals) == 0 {
			// 显式历史读空仍回流名 + 空数组并落盘新建消费者（对标真 Redis）；
			// ">" 空则跳过，整包保持 nil。
			if r.req.isNew {
				continue
			}
			vals = []protocol.Value{}
		}
		if werr := h.writeStream(ctx, r.req.key, r.s, r.expiry); werr != nil {
			return errValue(werr)
		}
		out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf(r.req.key),
			protocol.Value{Kind: protocol.KindArray, Elems: vals},
		}})
	}
	if out == nil {
		return protocol.Value{Kind: protocol.KindArray}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

type resolvedGroup struct {
	req    readgroupReq
	s      *datastruct.Stream
	expiry int64
	g      *datastruct.StreamGroup
}

// groupHasData 报告任一 key 是否有可投递数据（">" 看 LastID 之后，
// 显式看该消费者属主 PEL 中大于给定 ID 的条目）。
func groupHasData(rs []resolvedGroup, consumer string) bool {
	for _, r := range rs {
		if r.req.isNew {
			for _, e := range r.s.Entries {
				if e.ID.Compare(r.g.LastID) > 0 {
					return true
				}
			}
			continue
		}
		for _, p := range r.g.PEL {
			if p.Consumer == consumer && p.ID.Compare(r.req.explicit) > 0 {
				return true
			}
		}
	}
	return false
}

// deliverNew 投递 LastID 之后的新消息；推进 LastID；非 NOACK 建 PEL（count=1）
// 并设消费者 active。
func (h *streamHandler) deliverNew(s *datastruct.Stream, g *datastruct.StreamGroup, c *datastruct.StreamConsumer, count int64, noack bool, nowMs uint64) []protocol.Value {
	var out []protocol.Value
	for i := range s.Entries {
		e := &s.Entries[i]
		if e.ID.Compare(g.LastID) <= 0 {
			continue
		}
		if count > 0 && int64(len(out)) >= count {
			break
		}
		out = append(out, streamEntryValue(*e))
		g.LastID = e.ID
		if !noack {
			g.UpsertPEL(&datastruct.StreamPEL{ID: e.ID, Consumer: c.Name, DeliveryMs: nowMs, Count: 1})
			c.ActiveMs = nowMs
			c.HasActive = true
		}
	}
	return out
}

// redeliver 重投属主==消费者的 PEL entries（ID 大于给定值），count+1 并刷新时刻。
func (h *streamHandler) redeliver(s *datastruct.Stream, g *datastruct.StreamGroup, c *datastruct.StreamConsumer, after datastruct.StreamID, count int64, nowMs uint64) []protocol.Value {
	byID := make(map[datastruct.StreamID]*datastruct.StreamEntry, len(s.Entries))
	for i := range s.Entries {
		byID[s.Entries[i].ID] = &s.Entries[i]
	}
	var out []protocol.Value
	for _, p := range g.PEL {
		if p.Consumer != c.Name || p.ID.Compare(after) <= 0 {
			continue
		}
		e, ok := byID[p.ID]
		if !ok {
			continue
		}
		if count > 0 && int64(len(out)) >= count {
			break
		}
		p.Count++
		p.DeliveryMs = nowMs
		c.ActiveMs = nowMs
		c.HasActive = true
		out = append(out, streamEntryValue(*e))
	}
	return out
}

func parseIntArg(v protocol.Value) (int64, bool) {
	s, ok := argString(v)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
