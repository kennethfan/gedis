package commands

import (
	"context"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *streamHandler) registerTrim(r *network.Router) {
	r.Register("XTRIM", h.xtrim)
}

// xtrim 实现 XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]。
// 精确修剪（~ 与 = 等价）；MAXLEN/MINID 修剪永不碰 max-deleted（仲裁定案）。
func (h *streamHandler) xtrim(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'xtrim' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	strategy, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	isMaxlen := strings.EqualFold(strategy, "MAXLEN")
	isMinid := strings.EqualFold(strategy, "MINID")
	if !isMaxlen && !isMinid {
		return errValueStr("ERR syntax error")
	}
	rest := args[2:]
	approx := false
	if s, ok := argString(rest[0]); ok && (s == "=" || s == "~") {
		approx = s == "~"
		rest = rest[1:]
	}
	if len(rest) < 1 {
		return errValueStr("ERR syntax error")
	}
	threshStr, ok := argString(rest[0])
	if !ok {
		if isMaxlen {
			return errValueStr("ERR value is not an integer or out of range")
		}
		return errValueStr("ERR Invalid stream ID specified as stream command argument")
	}
	rest = rest[1:]
	limit := int64(-1)
	if len(rest) > 0 {
		if !approx {
			return errValueStr("ERR syntax error, LIMIT cannot be used without the special ~ option")
		}
		if len(rest) != 2 {
			return errValueStr("ERR syntax error")
		}
		opt, ok := argString(rest[0])
		if !ok || !strings.EqualFold(opt, "LIMIT") {
			return errValueStr("ERR syntax error")
		}
		nStr, ok := argString(rest[1])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		n, err := strconv.ParseInt(nStr, 10, 64)
		if err != nil || n < 0 {
			return errValueStr("ERR value is not an integer or out of range")
		}
		limit = n
	}
	var maxlen int64
	var minid datastruct.StreamID
	if isMaxlen {
		n, err := strconv.ParseInt(threshStr, 10, 64)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
		if n < 0 {
			return errValueStr("ERR The MAXLEN argument must be >= 0.")
		}
		maxlen = n
	} else {
		id, _, err := datastruct.StreamParseID(threshStr)
		if err != nil {
			return errValue(err)
		}
		minid = id
	}
	s, expiry, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	var deleted int64
	if isMaxlen {
		for int64(len(s.Entries)) > maxlen {
			if limit >= 0 && deleted >= limit {
				break
			}
			s.Entries = s.Entries[1:]
			deleted++
		}
	} else {
		for len(s.Entries) > 0 && s.Entries[0].ID.Compare(minid) < 0 {
			if limit >= 0 && deleted >= limit {
				break
			}
			s.Entries = s.Entries[1:]
			deleted++
		}
	}
	if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: deleted}
}
