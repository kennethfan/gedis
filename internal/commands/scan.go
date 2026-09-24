package commands

import (
	"context"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterScan 注册全局 SCAN（HSCAN/SSCAN/ZSCAN 见各类型文件，语义已对齐）。
func RegisterScan(r *network.Router, kv KV) {
	h := &scanHandler{kv: kv}
	r.Register("SCAN", h.scan)
}

type scanHandler struct {
	kv KV
}

// scan 实现 SCAN cursor [MATCH pattern] [COUNT n] [TYPE type]。
// 全集按 allUserKeys 去重后排序，保证分页确定、可走完；TYPE 值与 TYPE 命令一致。
func (h *scanHandler) scan(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'scan' command")
	}
	cursorStr, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid cursor")
	}
	cursor, err := strconv.ParseInt(cursorStr, 10, 64)
	if err != nil || cursor < 0 {
		return errValueStr("ERR value is not an integer or out of range")
	}
	pattern := "*"
	var count int64 = 10
	typeFilter := ""
	for i := 1; i < len(args); i++ {
		name, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(name) {
		case "MATCH":
			i++
			if i >= len(args) {
				return errValueStr("ERR syntax error")
			}
			pattern, ok = argString(args[i])
			if !ok {
				return errValueStr("ERR syntax error")
			}
		case "COUNT":
			i++
			if i >= len(args) {
				return errValueStr("ERR syntax error")
			}
			n, ok := argString(args[i])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			count, err = strconv.ParseInt(n, 10, 64)
			if err != nil || count < 0 {
				return errValueStr("ERR value is not an integer or out of range")
			}
		case "TYPE":
			i++
			if i >= len(args) {
				return errValueStr("ERR syntax error")
			}
			typeFilter, ok = argString(args[i])
			if !ok {
				return errValueStr("ERR syntax error")
			}
		default:
			return errValueStr("ERR syntax error")
		}
	}
	empty := protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("0"),
		{Kind: protocol.KindArray, Elems: []protocol.Value{}},
	}}
	names, raws, err := allUserKeys(ctx, h.kv)
	if err != nil {
		return errValue(err)
	}
	type keyType struct {
		name string
		typ  byte
	}
	now := time.Now().UnixNano()
	typed := make([]keyType, 0, len(names))
	for i, name := range names {
		raw, err := h.kv.Get(ctx, raws[i])
		if err != nil {
			continue
		}
		e, err := datastruct.Decode(raw)
		if err != nil {
			continue
		}
		if e.Expiry != 0 && now >= e.Expiry {
			continue
		}
		typed = append(typed, keyType{name: name, typ: e.Type})
	}
	sort.Slice(typed, func(a, b int) bool { return typed[a].name < typed[b].name })
	matched := make([]string, 0, len(typed))
	for _, kt := range typed {
		if typeFilter != "" && !strings.EqualFold(typeName(kt.typ), typeFilter) {
			continue
		}
		ok, merr := path.Match(pattern, kt.name)
		if merr != nil || !ok {
			continue
		}
		matched = append(matched, kt.name)
	}
	if cursor > int64(len(matched)) {
		cursor = int64(len(matched))
	}
	end := cursor + count
	var next int64
	if end >= int64(len(matched)) {
		end = int64(len(matched))
		next = 0
	} else {
		next = end
	}
	flat := make([]protocol.Value, 0, end-cursor)
	for _, name := range matched[cursor:end] {
		flat = append(flat, protocol.BulkOf(name))
	}
	if len(matched) == 0 {
		return empty
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(strconv.FormatInt(next, 10)),
		{Kind: protocol.KindArray, Elems: flat},
	}}
}
