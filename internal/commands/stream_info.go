package commands

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *streamHandler) registerInfo(r *network.Router) {
	r.Register("XINFO", h.xinfo)
	r.Register("XGROUP", h.xgroup)
	r.Register("XACK", h.xack)
}

var nilBulk = protocol.Value{Kind: protocol.KindBulkString}

// groupLag 算 lag：entries-read 已知时为 added-read（可为负），
// 否则为 LastID 之后现存 entry 数。
func groupLag(s *datastruct.Stream, g *datastruct.StreamGroup) (int64, bool) {
	if g.HasRead {
		return int64(s.Added) - int64(g.EntriesRead), true
	}
	var n int64
	for _, e := range s.Entries {
		if g.LastID.Compare(e.ID) < 0 {
			n++
		}
	}
	return n, true
}

func optValue(elems []protocol.Value, key string) (protocol.Value, bool) {
	for i := 0; i+1 < len(elems); i += 2 {
		if elems[i].S == key {
			return elems[i+1], true
		}
	}
	return protocol.Value{}, false
}

func (h *streamHandler) xinfo(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'xinfo' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "STREAM":
		return h.xinfoStream(ctx, args[1:])
	case "GROUPS":
		return h.xinfoGroups(ctx, args[1:])
	case "CONSUMERS":
		return h.xinfoConsumers(ctx, args[1:])
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s'. Try XINFO HELP.", sub))
	}
}

func (h *streamHandler) xinfoStream(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'xinfo' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	full := false
	count := int64(0)
	if len(args) > 1 {
		fullOpt, ok := argString(args[1])
		if !ok || !strings.EqualFold(fullOpt, "FULL") {
			return errValueStr("ERR unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.")
		}
		full = true
		if len(args) > 2 {
			if len(args) != 4 {
				return errValueStr("ERR unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.")
			}
			cntOpt, ok := argString(args[2])
			if !ok || !strings.EqualFold(cntOpt, "COUNT") {
				return errValueStr("ERR unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.")
			}
			nStr, ok := argString(args[3])
			if !ok {
				return errValueStr("ERR value is not an integer or out of range")
			}
			n, err := strconv.ParseInt(nStr, 10, 64)
			if err != nil {
				return errValueStr("ERR value is not an integer or out of range")
			}
			count = n
		}
	}
	s, _, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return errValueStr("ERR no such key")
		}
		return errValue(err)
	}
	lastID := "0-0"
	if s.HasLast {
		lastID = s.Last.String()
	}
	firstID := "0-0"
	if len(s.Entries) > 0 {
		firstID = s.Entries[0].ID.String()
	}
	out := []protocol.Value{
		protocol.BulkOf("length"), {Kind: protocol.KindInteger, I: int64(len(s.Entries))},
		protocol.BulkOf("radix-tree-keys"), {Kind: protocol.KindInteger, I: 0},
		protocol.BulkOf("radix-tree-nodes"), {Kind: protocol.KindInteger, I: 0},
		protocol.BulkOf("last-generated-id"), protocol.BulkOf(lastID),
		protocol.BulkOf("max-deleted-entry-id"), protocol.BulkOf(s.MaxDeleted.String()),
		protocol.BulkOf("entries-added"), {Kind: protocol.KindInteger, I: int64(s.Added)},
		protocol.BulkOf("recorded-first-entry-id"), protocol.BulkOf(firstID),
	}
	if !full {
		first, last := nilBulk, nilBulk
		if len(s.Entries) > 0 {
			first = streamEntryValue(s.Entries[0])
			last = streamEntryValue(s.Entries[len(s.Entries)-1])
		}
		out = append(out,
			protocol.BulkOf("groups"), protocol.Value{Kind: protocol.KindInteger, I: int64(len(s.Groups))},
			protocol.BulkOf("first-entry"), first,
			protocol.BulkOf("last-entry"), last,
		)
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	}
	entries := make([]protocol.Value, 0, len(s.Entries))
	for _, e := range s.Entries {
		if count > 0 && int64(len(entries)) >= count {
			break
		}
		entries = append(entries, streamEntryValue(e))
	}
	groups := make([]protocol.Value, 0, len(s.Groups))
	for _, g := range s.Groups {
		groups = append(groups, h.groupDetailValue(s, g))
	}
	out = append(out,
		protocol.BulkOf("entries"), protocol.Value{Kind: protocol.KindArray, Elems: entries},
		protocol.BulkOf("groups"), protocol.Value{Kind: protocol.KindArray, Elems: groups},
	)
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *streamHandler) groupDetailValue(s *datastruct.Stream, g *datastruct.StreamGroup) protocol.Value {
	readV, lagV := nilBulk, nilBulk
	if g.HasRead {
		readV = protocol.Value{Kind: protocol.KindInteger, I: int64(g.EntriesRead)}
	}
	if lag, ok := groupLag(s, g); ok {
		lagV = protocol.Value{Kind: protocol.KindInteger, I: lag}
	}
	consumers := make([]protocol.Value, 0, len(g.Consumers))
	for _, c := range g.Consumers {
		consumers = append(consumers, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf("name"), protocol.BulkOf(c),
			protocol.BulkOf("seen-time"), protocol.Value{Kind: protocol.KindInteger, I: 0},
			protocol.BulkOf("active-time"), protocol.Value{Kind: protocol.KindInteger, I: -1},
			protocol.BulkOf("pel-count"), protocol.Value{Kind: protocol.KindInteger, I: 0},
			protocol.BulkOf("pending"), protocol.Value{Kind: protocol.KindArray},
		}})
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("name"), protocol.BulkOf(g.Name),
		protocol.BulkOf("last-delivered-id"), protocol.BulkOf(g.LastID.String()),
		protocol.BulkOf("entries-read"), readV,
		protocol.BulkOf("lag"), lagV,
		protocol.BulkOf("pel-count"), protocol.Value{Kind: protocol.KindInteger, I: 0},
		protocol.BulkOf("pending"), protocol.Value{Kind: protocol.KindArray},
		protocol.BulkOf("consumers"), protocol.Value{Kind: protocol.KindArray, Elems: consumers},
	}}
}

func (h *streamHandler) xinfoGroups(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'xinfo' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	s, _, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return errValueStr("ERR no such key")
		}
		return errValue(err)
	}
	out := make([]protocol.Value, 0, len(s.Groups))
	for _, g := range s.Groups {
		readV, lagV := nilBulk, nilBulk
		if g.HasRead {
			readV = protocol.Value{Kind: protocol.KindInteger, I: int64(g.EntriesRead)}
		}
		if lag, ok := groupLag(s, g); ok {
			lagV = protocol.Value{Kind: protocol.KindInteger, I: lag}
		}
		out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf("name"), protocol.BulkOf(g.Name),
			protocol.BulkOf("consumers"), protocol.Value{Kind: protocol.KindInteger, I: int64(len(g.Consumers))},
			protocol.BulkOf("pending"), protocol.Value{Kind: protocol.KindInteger, I: 0},
			protocol.BulkOf("last-delivered-id"), protocol.BulkOf(g.LastID.String()),
			protocol.BulkOf("entries-read"), readV,
			protocol.BulkOf("lag"), lagV,
		}})
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *streamHandler) xinfoConsumers(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'xinfo' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	s, _, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return errValueStr("ERR no such key")
		}
		return errValue(err)
	}
	g := findGroup(s, groupName)
	if g == nil {
		return errValueStr(fmt.Sprintf("NOGROUP No such consumer group '%s' for key name '%s'", groupName, key))
	}
	out := make([]protocol.Value, 0, len(g.Consumers))
	for _, c := range g.Consumers {
		out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf("name"), protocol.BulkOf(c),
			protocol.BulkOf("pending"), protocol.Value{Kind: protocol.KindInteger, I: 0},
			protocol.BulkOf("idle"), protocol.Value{Kind: protocol.KindInteger, I: 0},
			protocol.BulkOf("inactive"), protocol.Value{Kind: protocol.KindInteger, I: -1},
		}})
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func findGroup(s *datastruct.Stream, name string) *datastruct.StreamGroup {
	for _, g := range s.Groups {
		if g.Name == name {
			return g
		}
	}
	return nil
}

func nogroupErr(group, key string) protocol.Value {
	return errValueStr(fmt.Sprintf("NOGROUP No such consumer group '%s' for key name '%s'", group, key))
}

const xgroupNeedKeyErr = "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."

func (h *streamHandler) xgroup(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'xgroup' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "CREATE":
		return h.xgroupCreate(ctx, args[1:])
	case "SETID":
		return h.xgroupSetID(ctx, args[1:])
	case "DESTROY":
		return h.xgroupDestroy(ctx, args[1:])
	case "CREATECONSUMER":
		return h.xgroupCreateConsumer(ctx, args[1:])
	case "DELCONSUMER":
		return h.xgroupDelConsumer(ctx, args[1:])
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s'. Try XGROUP HELP.", sub))
	}
}

// parseGroupID 解析组游标：$ 为当前 top（空流 0-0），其余走严格 ID（自动形非法）。
func parseGroupID(s string, st *datastruct.Stream) (datastruct.StreamID, error) {
	if s == "$" {
		if st != nil && st.HasLast {
			return st.Last, nil
		}
		return datastruct.StreamID{}, nil
	}
	id, auto, err := datastruct.StreamParseID(s)
	if err != nil || auto {
		return datastruct.StreamID{}, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}
	return id, nil
}

func (h *streamHandler) xgroupCreate(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 || len(args) > 6 {
		return errValueStr("ERR wrong number of arguments for 'xgroup|create' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	idStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR Invalid stream ID specified as stream command argument")
	}
	mkstream := false
	var entriesRead *uint64
	rest := args[3:]
	for len(rest) > 0 {
		opt, ok := argString(rest[0])
		if !ok {
			return errValueStr("ERR wrong number of arguments for 'xgroup|create' command")
		}
		switch {
		case strings.EqualFold(opt, "MKSTREAM"):
			mkstream = true
			rest = rest[1:]
		case strings.EqualFold(opt, "ENTRIESREAD"):
			if len(rest) != 2 {
				return errValueStr("ERR wrong number of arguments for 'xgroup|create' command")
			}
			nStr, ok := argString(rest[1])
			if !ok {
				return errValueStr("ERR value is not an integer or out of range")
			}
			n, err := strconv.ParseInt(nStr, 10, 64)
			if err != nil || n < 0 {
				return errValueStr("ERR value is not an integer or out of range")
			}
			u := uint64(n)
			entriesRead = &u
			rest = rest[2:]
		default:
			return errValueStr("ERR wrong number of arguments for 'xgroup|create' command")
		}
	}
	s, expiry, err := h.readStream(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		if !mkstream {
			return errValueStr(xgroupNeedKeyErr)
		}
		s = datastruct.StreamNew()
		expiry = 0
	}
	if findGroup(s, groupName) != nil {
		return errValueStr("BUSYGROUP Consumer Group name already exists")
	}
	id, err := parseGroupID(idStr, s)
	if err != nil {
		return errValue(err)
	}
	g := &datastruct.StreamGroup{Name: groupName, LastID: id}
	if entriesRead != nil {
		g.EntriesRead = *entriesRead
		g.HasRead = true
	}
	s.Groups = append(s.Groups, g)
	if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *streamHandler) xgroupSetID(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 && len(args) != 5 {
		return errValueStr("ERR wrong number of arguments for 'xgroup|setid' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	idStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR Invalid stream ID specified as stream command argument")
	}
	var entriesRead *uint64
	if len(args) == 5 {
		opt, ok := argString(args[3])
		if !ok || !strings.EqualFold(opt, "ENTRIESREAD") {
			return errValueStr("ERR wrong number of arguments for 'xgroup|setid' command")
		}
		nStr, ok := argString(args[4])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		n, err := strconv.ParseInt(nStr, 10, 64)
		if err != nil || n < 0 {
			return errValueStr("ERR value is not an integer or out of range")
		}
		u := uint64(n)
		entriesRead = &u
	}
	s, expiry, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return errValueStr(xgroupNeedKeyErr)
		}
		return errValue(err)
	}
	g := findGroup(s, groupName)
	if g == nil {
		return nogroupErr(groupName, key)
	}
	id, err := parseGroupID(idStr, s)
	if err != nil {
		return errValue(err)
	}
	g.LastID = id
	if entriesRead != nil {
		g.EntriesRead = *entriesRead
		g.HasRead = true
	}
	if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *streamHandler) xgroupDestroy(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'xgroup|destroy' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	s, expiry, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return errValueStr(xgroupNeedKeyErr)
		}
		return errValue(err)
	}
	for i, g := range s.Groups {
		if g.Name == groupName {
			s.Groups = append(s.Groups[:i], s.Groups[i+1:]...)
			if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
				return errValue(werr)
			}
			return protocol.Value{Kind: protocol.KindInteger, I: 1}
		}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 0}
}

func (h *streamHandler) xgroupCreateConsumer(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'xgroup|createconsumer' command")
	}
	key, _ := argString(args[0])
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	consumer, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid consumer")
	}
	s, expiry, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return errValueStr(xgroupNeedKeyErr)
		}
		return errValue(err)
	}
	g := findGroup(s, groupName)
	if g == nil {
		return nogroupErr(groupName, key)
	}
	for _, c := range g.Consumers {
		if c == consumer {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
	}
	g.Consumers = append(g.Consumers, consumer)
	if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 1}
}

func (h *streamHandler) xgroupDelConsumer(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'xgroup|delconsumer' command")
	}
	key, _ := argString(args[0])
	groupName, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid group")
	}
	consumer, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid consumer")
	}
	s, expiry, err := h.readStream(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return errValueStr(xgroupNeedKeyErr)
		}
		return errValue(err)
	}
	g := findGroup(s, groupName)
	if g == nil {
		return nogroupErr(groupName, key)
	}
	for i, c := range g.Consumers {
		if c == consumer {
			g.Consumers = append(g.Consumers[:i], g.Consumers[i+1:]...)
			if werr := h.writeStream(ctx, key, s, expiry); werr != nil {
				return errValue(werr)
			}
			break
		}
	}
	// M2-1 不跟踪 PEL：删消费者恒返丢弃 pending 数 0。
	return protocol.Value{Kind: protocol.KindInteger, I: 0}
}

// xack 是桩：缺 key 返 0，已存在非 stream 返 WRONGTYPE，只校验 ID 合法（M2-2 才跟踪 PEL）。
func (h *streamHandler) xack(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'xack' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	_, _, err := h.readStream(ctx, key)
	if err != nil && !isNotFound(err) {
		return errValue(err)
	}
	for _, a := range args[2:] {
		s, ok := argString(a)
		if !ok {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
		_, auto, err := datastruct.StreamParseID(s)
		if err != nil || auto {
			return errValueStr("ERR Invalid stream ID specified as stream command argument")
		}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 0}
}
