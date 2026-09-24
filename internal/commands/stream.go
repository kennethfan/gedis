package commands

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterStream 注册 Stream 基础命令；XREAD/修剪/元信息由同包其它文件追加。
func RegisterStream(r *network.Router, kv KV, stats *network.Stats) {
	h := &streamHandler{kv: kv, stats: stats}
	r.Register("XADD", h.xadd)
	r.Register("XLEN", h.xlen)
	r.Register("XRANGE", h.xrange)
	r.Register("XREVRANGE", h.xrevrange)
	r.Register("XDEL", h.xdel)
	h.registerRead(r)
	h.registerTrim(r)
	h.registerInfo(r)
	h.registerGroup(r)
	h.registerClaim(r)
	h.registerXClaim(r)
}

type streamHandler struct {
	kv    KV
	stats *network.Stats
}

func (h *streamHandler) readStream(ctx context.Context, key string) (*datastruct.Stream, int64, error) {
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil {
		return nil, 0, err
	}
	if e.Type != datastruct.TypeStream {
		return nil, 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	s, err := datastruct.DecodeStream(e.Payload)
	if err != nil {
		return nil, 0, err
	}
	return s, e.Expiry, nil
}

// writeStream 回写全量；空 stream 也保留 key（对标真 Redis：XDEL/XTRIM 删光后 key 仍在）。
func (h *streamHandler) writeStream(ctx context.Context, key string, s *datastruct.Stream, expiry int64) error {
	return h.kv.Set(ctx, datastruct.StreamKey(key),
		datastruct.Encode(datastruct.TypeStream, expiry, datastruct.EncodeStream(s)))
}

func (h *streamHandler) xadd(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'xadd' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	idStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR Invalid stream ID specified as stream command argument")
	}
	fields := make([]string, 0, len(args)-2)
	for _, a := range args[2:] {
		f, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid field")
		}
		fields = append(fields, f)
	}
	if len(fields)%2 != 0 {
		return errValueStr("ERR wrong number of arguments for 'xadd' command")
	}
	s, expiry, err := h.readStream(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		s = datastruct.StreamNew()
	}
	id, err := resolveAddID(idStr, s)
	if err != nil {
		return errValue(err)
	}
	s.Add(id, fields)
	s.Last = id
	s.HasLast = true
	s.Added++
	if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.BulkOf(id.String())
}

// resolveAddID 按 Redis 规则确定 XADD 的 entry-id：
// "*" 全自动；"ms-*" seq 自动；显式 ID 必须大于 last（空流则大于 0-0）。
func resolveAddID(idStr string, s *datastruct.Stream) (datastruct.StreamID, error) {
	id, auto, err := datastruct.StreamParseID(idStr)
	if err != nil {
		return datastruct.StreamID{}, err
	}
	smaller := fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	if !auto {
		if !s.HasLast {
			if id == (datastruct.StreamID{}) {
				return datastruct.StreamID{}, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
			}
			return id, nil
		}
		if id.Compare(s.Last) <= 0 {
			return datastruct.StreamID{}, smaller
		}
		return id, nil
	}
	if idStr == "*" {
		ms := uint64(time.Now().UnixMilli())
		if s.HasLast && ms <= s.Last.Ms {
			ms = s.Last.Ms
			if s.Last.Seq == ^uint64(0) {
				return datastruct.StreamID{}, smaller
			}
			return datastruct.StreamID{Ms: ms, Seq: s.Last.Seq + 1}, nil
		}
		return datastruct.StreamID{Ms: ms}, nil
	}
	// "ms-*" 形。
	if !s.HasLast {
		if id.Ms == 0 {
			return datastruct.StreamID{Ms: 0, Seq: 1}, nil
		}
		return datastruct.StreamID{Ms: id.Ms}, nil
	}
	switch {
	case id.Ms > s.Last.Ms:
		return datastruct.StreamID{Ms: id.Ms}, nil
	case id.Ms == s.Last.Ms:
		if s.Last.Seq == ^uint64(0) {
			return datastruct.StreamID{}, smaller
		}
		return datastruct.StreamID{Ms: id.Ms, Seq: s.Last.Seq + 1}, nil
	default:
		return datastruct.StreamID{}, smaller
	}
}

func (h *streamHandler) xlen(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'xlen' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	s, _, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(s.Entries))}
}

func (h *streamHandler) xrange(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.rangeCmd(ctx, args, false)
}

func (h *streamHandler) xrevrange(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.rangeCmd(ctx, args, true)
}

func (h *streamHandler) rangeCmd(ctx context.Context, args []protocol.Value, reverse bool) protocol.Value {
	name := "'xrange' command"
	if reverse {
		name = "'xrevrange' command"
	}
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for " + name)
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	startStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR Invalid stream ID specified as stream command argument")
	}
	endStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR Invalid stream ID specified as stream command argument")
	}
	start, err := parseRangeBound(startStr, true)
	if err != nil {
		return errValue(err)
	}
	end, err := parseRangeBound(endStr, false)
	if err != nil {
		return errValue(err)
	}
	count := int64(-1)
	if len(args) > 3 {
		var perr error
		count, perr = parseRangeCount(args[3:])
		if perr != nil {
			return errValue(perr)
		}
	}
	s, _, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	var out []protocol.Value
	lo, hi := start, end
	if reverse {
		lo, hi = end, start
	}
	collect := func(e datastruct.StreamEntry) {
		if e.ID.Compare(lo) < 0 || e.ID.Compare(hi) > 0 {
			return
		}
		if count >= 0 && int64(len(out)) >= count {
			return
		}
		fv := make([]protocol.Value, len(e.Fields))
		for i, f := range e.Fields {
			fv[i] = protocol.BulkOf(f)
		}
		out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf(e.ID.String()),
			protocol.Value{Kind: protocol.KindArray, Elems: fv},
		}})
	}
	if !reverse {
		for _, e := range s.Entries {
			collect(e)
		}
	} else {
		for i := len(s.Entries) - 1; i >= 0; i-- {
			collect(s.Entries[i])
		}
	}
	if out == nil {
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

// parseRangeBound 解析 XRANGE 边界："-"/"+" 为极值，其余走严格 ID（"(" 前缀不支持）。
func parseRangeBound(s string, isStart bool) (datastruct.StreamID, error) {
	if s == "-" {
		return datastruct.StreamMinID, nil
	}
	if s == "+" {
		return datastruct.StreamMaxID, nil
	}
	_ = isStart
	id, _, err := datastruct.StreamParseID(s)
	return id, err
}

func parseRangeCount(rest []protocol.Value) (int64, error) {
	if len(rest) != 2 {
		return 0, fmt.Errorf("ERR syntax error")
	}
	opt, ok := argString(rest[0])
	if !ok || (opt != "COUNT" && opt != "count" && opt != "Count") {
		return 0, fmt.Errorf("ERR syntax error")
	}
	nStr, ok := argString(rest[1])
	if !ok {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}
	n, err := strconv.ParseInt(nStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}
	return n, nil
}

func (h *streamHandler) xdel(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'xdel' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	// 先全量解析 ID：任一非法则整命令报错、不做部分删除。
	ids := make([]datastruct.StreamID, 0, len(args)-1)
	for _, a := range args[1:] {
		s, ok := argString(a)
		if !ok {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
		id, _, err := datastruct.StreamParseID(s)
		if err != nil {
			return errValue(err)
		}
		ids = append(ids, id)
	}
	s, expiry, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	var deleted int64
	for _, id := range ids {
		for i, e := range s.Entries {
			if e.ID == id {
				s.Entries = append(s.Entries[:i], s.Entries[i+1:]...)
				deleted++
				if s.MaxDeleted.Compare(id) < 0 {
					s.MaxDeleted = id
				}
				break
			}
		}
	}
	if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: deleted}
}
