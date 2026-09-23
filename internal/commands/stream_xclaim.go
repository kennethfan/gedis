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

func (h *streamHandler) registerXClaim(r *network.Router) {
	r.Register("XCLAIM", h.xclaim)
	r.Register("XAUTOCLAIM", h.xautoclaim)
}

func findStreamEntry(s *datastruct.Stream, id datastruct.StreamID) *datastruct.StreamEntry {
	for i := range s.Entries {
		if s.Entries[i].ID == id {
			return &s.Entries[i]
		}
	}
	return nil
}

func (h *streamHandler) xclaim(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 5 {
		return errValueStr("ERR wrong number of arguments for 'xclaim' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	consumer, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid consumer")
	}
	minIdleStr, ok := argString(args[3])
	if !ok {
		return errValueStr("ERR Invalid min-idle-time argument for XCLAIM")
	}
	minIdle, err := strconv.ParseInt(minIdleStr, 10, 64)
	if err != nil {
		return errValueStr("ERR Invalid min-idle-time argument for XCLAIM")
	}
	var ids []datastruct.StreamID
	var idleSet, timeSet, retrySet bool
	var idleVal, timeVal, retryVal int64
	var force, justid bool
	for i := 4; i < len(args); i++ {
		tok, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
		// 选项名大小写不敏感；非选项 token 走 ID 解析（对标真 Redis）。
		switch strings.ToUpper(tok) {
		case "FORCE":
			force = true
			continue
		case "JUSTID":
			justid = true
			continue
		case "IDLE", "TIME", "RETRYCOUNT":
			up := strings.ToUpper(tok)
			if i+1 >= len(args) {
				return errValueStr("ERR syntax error")
			}
			vStr, ok := argString(args[i+1])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			v, verr := strconv.ParseInt(vStr, 10, 64)
			if verr != nil {
				switch up {
				case "IDLE":
					return errValueStr("ERR Invalid IDLE option argument for XCLAIM")
				case "TIME":
					return errValueStr("ERR Invalid TIME option argument for XCLAIM")
				default:
					return errValueStr("ERR Invalid RETRYCOUNT option argument for XCLAIM")
				}
			}
			switch up {
			case "IDLE":
				idleSet, idleVal = true, v
			case "TIME":
				timeSet, timeVal = true, v
			default:
				retrySet, retryVal = true, v
			}
			i++
			continue
		}
		id, auto, perr := datastruct.StreamParseID(tok)
		if perr != nil || auto {
			return errValueStr(fmt.Sprintf("ERR Unrecognized XCLAIM option '%s'", tok))
		}
		ids = append(ids, id)
	}
	s, expiry, g, errVal, ok, _ := h.loadGroup(ctx, key, groupName)
	if !ok {
		return errVal
	}
	now := time.Now().UnixMilli()
	nowMs := uint64(now)
	out := make([]protocol.Value, 0)
	for _, id := range ids {
		p := g.FindPEL(id)
		fresh := false
		if p == nil {
			if !force {
				continue
			}
			if findStreamEntry(s, id) == nil {
				continue
			}
			p = &datastruct.StreamPEL{ID: id, Consumer: consumer, DeliveryMs: nowMs, Count: 1}
			g.UpsertPEL(p)
			fresh = true
		} else {
			if int64(p.DeliveryMs) > now-minIdle {
				continue
			}
		}
		c := &g.Consumers[g.EnsureConsumer(consumer, nowMs)]
		c.SeenMs = nowMs
		c.ActiveMs = nowMs
		c.HasActive = true
		p.Consumer = consumer
		if justid {
			// JUSTID 只迁移 owner，不碰 time/count。
			out = append(out, protocol.BulkOf(id.String()))
			continue
		}
		if timeSet {
			p.DeliveryMs = uint64(timeVal)
		} else {
			p.DeliveryMs = nowMs
		}
		if idleSet {
			p.DeliveryMs = uint64(now - idleVal)
		}
		if retrySet {
			p.Count = uint64(retryVal)
		} else if !fresh {
			p.Count++
		}
		out = append(out, streamEntryValue(*findStreamEntry(s, id)))
	}
	if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *streamHandler) xautoclaim(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 5 {
		return errValueStr("ERR wrong number of arguments for 'xautoclaim' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	consumer, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid consumer")
	}
	minIdleStr, ok := argString(args[3])
	if !ok {
		return errValueStr("ERR Invalid min-idle-time argument for XAUTOCLAIM")
	}
	minIdle, err := strconv.ParseInt(minIdleStr, 10, 64)
	if err != nil {
		return errValueStr("ERR Invalid min-idle-time argument for XAUTOCLAIM")
	}
	startStr, ok := argString(args[4])
	if !ok {
		return errValueStr("ERR Invalid stream ID specified as stream command argument")
	}
	var start datastruct.StreamID
	if startStr == "-" {
		start = datastruct.StreamMinID
	} else {
		id, auto, perr := datastruct.StreamParseID(startStr)
		if perr != nil || auto {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
		start = id
	}
	count := int64(100)
	justid := false
	for i := 5; i < len(args); i++ {
		opt, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(opt) {
		case "COUNT":
			if i+1 >= len(args) {
				return errValueStr("ERR syntax error")
			}
			nStr, ok := argString(args[i+1])
			if !ok {
				return errValueStr("ERR value is not an integer or out of range")
			}
			n, nerr := strconv.ParseInt(nStr, 10, 64)
			if nerr != nil {
				return errValueStr("ERR value is not an integer or out of range")
			}
			if n <= 0 {
				return errValueStr("ERR COUNT must be > 0")
			}
			count = n
			i++
		case "JUSTID":
			justid = true
		default:
			return errValueStr("ERR syntax error")
		}
	}
	s, expiry, g, errVal, ok, _ := h.loadGroup(ctx, key, groupName)
	if !ok {
		return errVal
	}
	now := time.Now().UnixMilli()
	nowMs := uint64(now)
	entries := make([]protocol.Value, 0)
	orphans := make([]protocol.Value, 0)
	var visited int64
	var lastVisited *datastruct.StreamID
	for i := range g.PEL {
		p := g.PEL[i]
		if p.ID.Compare(start) < 0 {
			continue
		}
		if visited >= count {
			break
		}
		visited++
		cp := p.ID
		lastVisited = &cp
		e := findStreamEntry(s, p.ID)
		if e == nil {
			orphans = append(orphans, protocol.BulkOf(p.ID.String()))
			continue
		}
		if int64(p.DeliveryMs) > now-minIdle {
			continue
		}
		p.Consumer = consumer
		c := &g.Consumers[g.EnsureConsumer(consumer, nowMs)]
		c.SeenMs = nowMs
		c.ActiveMs = nowMs
		c.HasActive = true
		if justid {
			// JUSTID 只迁移 owner，不碰 time/count（对标真 Redis）。
			entries = append(entries, protocol.BulkOf(p.ID.String()))
			continue
		}
		p.DeliveryMs = nowMs
		p.Count++
		if justid {
			entries = append(entries, protocol.BulkOf(p.ID.String()))
		} else {
			entries = append(entries, streamEntryValue(*e))
		}
	}
	// 移出 orphan 并定 cursor：末 visit 之后首个 PEL ID，无则 0-0。
	keep := g.PEL[:0]
	orphanSet := make(map[datastruct.StreamID]bool, len(orphans))
	for _, o := range orphans {
		id, _, _ := datastruct.StreamParseID(string(o.Bulk))
		orphanSet[id] = true
	}
	for _, p := range g.PEL {
		if !orphanSet[p.ID] {
			keep = append(keep, p)
		}
	}
	g.PEL = keep
	cursor := datastruct.StreamMinID
	if lastVisited != nil {
		cursor = datastruct.StreamMaxID
		for _, p := range g.PEL {
			if lastVisited.Compare(p.ID) < 0 {
				cursor = p.ID
				break
			}
		}
		if cursor == datastruct.StreamMaxID {
			cursor = datastruct.StreamMinID
		}
	}
	if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(cursor.String()),
		protocol.Value{Kind: protocol.KindArray, Elems: entries},
		protocol.Value{Kind: protocol.KindArray, Elems: orphans},
	}}
}
