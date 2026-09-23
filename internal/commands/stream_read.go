package commands

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *streamHandler) registerRead(r *network.Router) {
	r.Register("XREAD", h.xread)
}

type streamReadReq struct {
	key   string
	after datastruct.StreamID
}

// xread 实现 XREAD [COUNT n] [BLOCK ms] STREAMS key… id…。
// COUNT<=0 不限数（真值）；BLOCK 0 表无限等；"$" 解析为命令开始时的 last-id。
func (h *streamHandler) xread(ctx context.Context, args []protocol.Value) protocol.Value {
	count := int64(-1)
	block := int64(-1)
	i := 0
	for i < len(args) {
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
			n, err := strconv.ParseInt(nStr, 10, 64)
			if err != nil {
				return errValueStr("ERR value is not an integer or out of range")
			}
			count = n
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return errValueStr("ERR syntax error")
			}
			tStr, ok := argString(args[i+1])
			if !ok {
				return errValueStr("ERR timeout is not an integer or out of range")
			}
			ms, err := strconv.ParseInt(tStr, 10, 64)
			if err != nil {
				return errValueStr("ERR timeout is not an integer or out of range")
			}
			if ms < 0 {
				return errValueStr("ERR timeout is negative")
			}
			block = ms
			i += 2
		default:
			// XREAD 无 GROUP；选项位出现 GROUP → wrong-number（对标真 Redis）。
			if strings.EqualFold(opt, "GROUP") {
				return errValueStr("ERR wrong number of arguments for 'xread' command")
			}
			goto streams
		}
	}
streams:
	if i >= len(args) {
		return errValueStr("ERR wrong number of arguments for 'xread' command")
	}
	kw, ok := argString(args[i])
	if !ok || strings.ToUpper(kw) != "STREAMS" {
		return errValueStr("ERR syntax error")
	}
	rest := args[i+1:]
	if len(rest) < 2 {
		return errValueStr("ERR wrong number of arguments for 'xread' command")
	}
	if len(rest)%2 != 0 {
		return errValueStr("ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.")
	}
	nh := len(rest) / 2
	reqs := make([]streamReadReq, 0, nh)
	for j := 0; j < nh; j++ {
		key, ok := argString(rest[j])
		if !ok {
			return errValueStr("ERR invalid key")
		}
		idStr, ok := argString(rest[nh+j])
		if !ok {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
		if idStr == ">" {
			return errValueStr("ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.")
		}
		var after datastruct.StreamID
		if idStr == "$" {
			s, _, err := h.readStream(ctx, key)
			if err != nil {
				if !isNotFound(err) {
					return errValue(err)
				}
			} else if s.HasLast {
				after = s.Last
			}
		} else {
			id, _, err := datastruct.StreamParseID(idStr)
			if err != nil {
				return errValue(err)
			}
			after = id
		}
		reqs = append(reqs, streamReadReq{key: key, after: after})
	}
	// 先做类型预检：命中非 stream key 直接 WRONGTYPE，不进入等待。
	for _, q := range reqs {
		_, _, err := h.readStream(ctx, q.key)
		if err != nil && !isNotFound(err) {
			return errValue(err)
		}
	}
	if out, ok := h.tryRead(ctx, reqs, count); ok {
		return out
	}
	if block < 0 {
		return protocol.Value{Kind: protocol.KindArray}
	}
	var deadline time.Time
	if block > 0 {
		deadline = time.Now().Add(time.Duration(block) * time.Millisecond)
	}
	blocked := false
	defer func() {
		if blocked {
			h.stats.DecBlocked()
		}
	}()
	for {
		if out, ok := h.tryRead(ctx, reqs, count); ok {
			return out
		}
		if block > 0 && !time.Now().Before(deadline) {
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
	}
}

// tryRead 取各 key 在 after 之后的新 entries；全无返回 ok=false。
func (h *streamHandler) tryRead(ctx context.Context, reqs []streamReadReq, count int64) (protocol.Value, bool) {
	var out []protocol.Value
	for _, q := range reqs {
		s, _, err := h.readStream(ctx, q.key)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return errValue(err), true
		}
		var entries []protocol.Value
		for _, e := range s.Entries {
			if e.ID.Compare(q.after) <= 0 {
				continue
			}
			if count > 0 && int64(len(entries)) >= count {
				break
			}
			entries = append(entries, streamEntryValue(e))
		}
		if len(entries) == 0 {
			continue
		}
		out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf(q.key),
			protocol.Value{Kind: protocol.KindArray, Elems: entries},
		}})
	}
	if out == nil {
		return protocol.Value{Kind: protocol.KindArray}, false
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}, true
}

func streamEntryValue(e datastruct.StreamEntry) protocol.Value {
	fv := make([]protocol.Value, len(e.Fields))
	for i, f := range e.Fields {
		fv[i] = protocol.BulkOf(f)
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(e.ID.String()),
		protocol.Value{Kind: protocol.KindArray, Elems: fv},
	}}
}
