package commands

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterGeneric 注册通用 key 命令 COPY/RENAME/RENAMENX 与 SORT。
func RegisterGeneric(r *network.Router, kv KV) {
	h := &genericHandler{kv: kv}
	r.Register("COPY", h.copy)
	r.Register("RENAME", h.renamePlain)
	r.Register("RENAMENX", h.renamenx)
	r.Register("SORT", h.sort)
}

type genericHandler struct {
	kv KV
	lh *listHandler
	sh *setHandler
	zh *zsetHandler
	hh *hashHandler
}

func (h *genericHandler) lists() *listHandler {
	if h.lh == nil {
		h.lh = &listHandler{kv: h.kv}
	}
	return h.lh
}

func (h *genericHandler) sets() *setHandler {
	if h.sh == nil {
		h.sh = &setHandler{kv: h.kv}
	}
	return h.sh
}

func (h *genericHandler) zsets() *zsetHandler {
	if h.zh == nil {
		h.zh = &zsetHandler{kv: h.kv}
	}
	return h.zh
}

func (h *genericHandler) hashes() *hashHandler {
	if h.hh == nil {
		h.hh = &hashHandler{kv: h.kv}
	}
	return h.hh
}

// prefixForType 把 Entry 类型映射到存储前缀（与 keyspace.go typePrefixes 同构）。
func prefixForType(t byte) string {
	switch t {
	case datastruct.TypeString:
		return "s:"
	case datastruct.TypeHash:
		return "h:"
	case datastruct.TypeList:
		return "l:"
	case datastruct.TypeSet:
		return "st:"
	case datastruct.TypeZSet:
		return "z:"
	}
	return ""
}

// deleteByRaw 按存储 key 删除，同时清理 hash field 过期 sidecar。
func (h *genericHandler) deleteByRaw(ctx context.Context, raw []byte, e datastruct.Entry, userKey string) {
	_ = h.kv.Delete(ctx, raw)
	if e.Type == datastruct.TypeHash {
		_ = h.kv.Delete(ctx, datastruct.HashExpKey(userKey))
	}
}

func (h *genericHandler) copy(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'copy' command")
	}
	src, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	dst, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	replace := false
	for i := 2; i < len(args); i++ {
		name, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(name) {
		case "REPLACE":
			replace = true
		case "DB":
			i++
			if i >= len(args) {
				return errValueStr("ERR syntax error")
			}
			dbStr, ok := argString(args[i])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			db, err := strconv.Atoi(dbStr)
			if err != nil || db != 0 {
				return errValueStr("ERR DB index is out of range")
			}
		default:
			return errValueStr("ERR syntax error")
		}
	}
	if src == dst {
		return errValueStr("ERR source and destination objects are the same")
	}
	_, se, err := lookupRaw(ctx, h.kv, src)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	if _, _, err := lookupRaw(ctx, h.kv, dst); err == nil {
		if !replace {
			return protocol.Value{Kind: protocol.KindInteger}
		}
	} else if !isNotFound(err) {
		return errValue(err)
	}
	srcRaw, err := h.kv.Get(ctx, []byte(prefixForType(se.Type)+src))
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	val := append([]byte(nil), srcRaw...)
	dstPrefix := prefixForType(se.Type)
	if dstPrefix == "" {
		return errValueStr("ERR unknown source type")
	}
	if _, de, err := lookupRaw(ctx, h.kv, dst); err == nil {
		h.deleteByRaw(ctx, []byte(prefixForType(de.Type)+dst), de, dst)
	} else if !isNotFound(err) {
		return errValue(err)
	}
	if err := h.kv.Set(ctx, []byte(dstPrefix+dst), val); err != nil {
		return errValue(err)
	}
	if se.Type == datastruct.TypeHash {
		if expRaw, err := h.kv.Get(ctx, datastruct.HashExpKey(src)); err == nil {
			_ = h.kv.Set(ctx, datastruct.HashExpKey(dst), append([]byte(nil), expRaw...))
		} else if !isNotFound(err) {
			return errValue(err)
		}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 1}
}

func (h *genericHandler) rename(ctx context.Context, args []protocol.Value, nx bool) protocol.Value {
	name := "rename"
	if nx {
		name = "renamenx"
	}
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for '" + name + "' command")
	}
	src, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	dst, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	if src == dst {
		if nx {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		if _, _, err := lookupRaw(ctx, h.kv, src); err != nil {
			if isNotFound(err) {
				return errValueStr("ERR no such key")
			}
			return errValue(err)
		}
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	}
	srcRaw, se, err := lookupRaw(ctx, h.kv, src)
	if err != nil {
		if isNotFound(err) {
			return errValueStr("ERR no such key")
		}
		return errValue(err)
	}
	if draw, de, err := lookupRaw(ctx, h.kv, dst); err == nil {
		if nx {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		h.deleteByRaw(ctx, draw, de, dst)
	} else if !isNotFound(err) {
		return errValue(err)
	}
	val, err := h.kv.Get(ctx, srcRaw)
	if err != nil {
		if isNotFound(err) {
			return errValueStr("ERR no such key")
		}
		return errValue(err)
	}
	dstPrefix := prefixForType(se.Type)
	if dstPrefix == "" {
		return errValueStr("ERR unknown source type")
	}
	if err := h.kv.Set(ctx, []byte(dstPrefix+dst), append([]byte(nil), val...)); err != nil {
		return errValue(err)
	}
	h.deleteByRaw(ctx, srcRaw, se, src)
	if se.Type == datastruct.TypeHash {
		if expRaw, err := h.kv.Get(ctx, datastruct.HashExpKey(src)); err == nil {
			_ = h.kv.Set(ctx, datastruct.HashExpKey(dst), append([]byte(nil), expRaw...))
			_ = h.kv.Delete(ctx, datastruct.HashExpKey(src))
		} else if !isNotFound(err) {
			return errValue(err)
		}
	}
	if nx {
		return protocol.Value{Kind: protocol.KindInteger, I: 1}
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *genericHandler) renamenx(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.rename(ctx, args, true)
}

func (h *genericHandler) renamePlain(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.rename(ctx, args, false)
}

type sortOptions struct {
	by      string
	bySet   bool
	limitOn bool
	off     int64
	cnt     int64
	gets    []string
	desc    bool
	alpha   bool
	store   string
	storeOn bool
}

func parseSortOptions(args []protocol.Value) (sortOptions, *protocol.Value) {
	var o sortOptions
	for i := 1; i < len(args); i++ {
		name, ok := argString(args[i])
		if !ok {
			v := errValueStr("ERR syntax error")
			return o, &v
		}
		switch strings.ToUpper(name) {
		case "BY":
			i++
			if i >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			pat, ok := argString(args[i])
			if !ok {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			o.by, o.bySet = pat, true
		case "LIMIT":
			if i+2 >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			offStr, ok1 := argString(args[i+1])
			cntStr, ok2 := argString(args[i+2])
			if !ok1 || !ok2 {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			off, err1 := strconv.ParseInt(offStr, 10, 64)
			cnt, err2 := strconv.ParseInt(cntStr, 10, 64)
			if err1 != nil || err2 != nil {
				v := errValueStr("ERR value is not an integer or out of range")
				return o, &v
			}
			o.limitOn = true
			o.off, o.cnt = off, cnt
			i += 2
		case "GET":
			i++
			if i >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			pat, ok := argString(args[i])
			if !ok {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			o.gets = append(o.gets, pat)
		case "ASC":
			o.desc = false
		case "DESC":
			o.desc = true
		case "ALPHA":
			o.alpha = true
		case "STORE":
			i++
			if i >= len(args) {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			dst, ok := argString(args[i])
			if !ok {
				v := errValueStr("ERR syntax error")
				return o, &v
			}
			o.store, o.storeOn = dst, true
		default:
			v := errValueStr("ERR syntax error")
			return o, &v
		}
	}
	return o, nil
}

// resolveSortPattern 解析 BY/GET 模板：首个 * 替换为元素，独立 # 即元素本身，
// 后缀 ->field 走 hash 取 field；返回 (值, 命中)。
func (h *genericHandler) resolveSortPattern(ctx context.Context, pattern, elem string) (string, bool) {
	key := pattern
	if pattern == "#" {
		return elem, true
	}
	if idx := strings.IndexByte(pattern, '*'); idx >= 0 {
		key = pattern[:idx] + elem + pattern[idx+1:]
	}
	field := ""
	if idx := strings.Index(key, "->"); idx >= 0 {
		field = key[idx+2:]
		key = key[:idx]
	}
	if field != "" {
		m, _, err := h.hashes().readHash(ctx, key)
		if err != nil {
			return "", false
		}
		v, ok := m[field]
		return v, ok
	}
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil || e.Type != datastruct.TypeString {
		return "", false
	}
	return string(e.Payload), true
}

type sortItem struct {
	elem  string
	score float64
	str   string
}

func (h *genericHandler) sort(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 {
		return errValueStr("ERR wrong number of arguments for 'sort' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	o, errReply := parseSortOptions(args)
	if errReply != nil {
		return *errReply
	}
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil {
		if isNotFound(err) {
			if o.storeOn {
				return protocol.Value{Kind: protocol.KindInteger}
			}
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	var elems []string
	switch e.Type {
	case datastruct.TypeList:
		elems, _, err = h.lists().readList(ctx, key)
	case datastruct.TypeSet:
		var set map[string]struct{}
		set, _, err = h.sets().readSet(ctx, key)
		if err == nil {
			elems = make([]string, 0, len(set))
			for m := range set {
				elems = append(elems, m)
			}
			sort.Strings(elems)
		}
	case datastruct.TypeZSet:
		var z map[string]float64
		z, _, err = h.zsets().readZSet(ctx, key)
		if err == nil {
			sorted := sortedZSet(z)
			elems = make([]string, 0, len(sorted))
			for _, p := range sorted {
				elems = append(elems, p.m)
			}
		}
	default:
		return errValueStr("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if err != nil {
		return errValue(err)
	}
	items := make([]sortItem, 0, len(elems))
	if !o.bySet {
		for _, el := range elems {
			if o.alpha {
				items = append(items, sortItem{elem: el, str: el})
				continue
			}
			f, perr := strconv.ParseFloat(el, 64)
			if perr != nil {
				return errValueStr("ERR One or more scores can't be converted into double")
			}
			items = append(items, sortItem{elem: el, score: f})
		}
	} else {
		for _, el := range elems {
			v, found := h.resolveSortPattern(ctx, o.by, el)
			if o.alpha {
				s := ""
				if found {
					s = v
				}
				items = append(items, sortItem{elem: el, str: s})
				continue
			}
			if !found {
				items = append(items, sortItem{elem: el, score: 0})
				continue
			}
			f, perr := strconv.ParseFloat(v, 64)
			if perr != nil {
				return errValueStr("ERR One or more scores can't be converted into double")
			}
			items = append(items, sortItem{elem: el, score: f})
		}
	}
	sort.SliceStable(items, func(a, b int) bool {
		if o.alpha {
			if items[a].str == items[b].str {
				return false
			}
			if o.desc {
				return items[a].str > items[b].str
			}
			return items[a].str < items[b].str
		}
		if items[a].score == items[b].score {
			return false
		}
		if o.desc {
			return items[a].score > items[b].score
		}
		return items[a].score < items[b].score
	})
	lo, hi := int64(0), int64(len(items))
	if o.limitOn {
		lo = o.off
		if lo < 0 {
			lo = 0
		}
		if lo > hi {
			lo = hi
		}
		if o.cnt < 0 {
			hi = int64(len(items))
		} else {
			hi = lo + o.cnt
			if hi > int64(len(items)) {
				hi = int64(len(items))
			}
		}
	}
	items = items[lo:hi]
	if o.storeOn {
		var out []string
		if len(o.gets) == 0 {
			out = make([]string, 0, len(items))
			for _, it := range items {
				out = append(out, it.elem)
			}
		} else {
			vals := h.collectGet(ctx, o.gets, items, true)
			out = make([]string, 0, len(vals))
			for _, v := range vals {
				out = append(out, string(v.Bulk))
			}
		}
		if draw, de, derr := lookupRaw(ctx, h.kv, o.store); derr == nil {
			h.deleteByRaw(ctx, draw, de, o.store)
		} else if !isNotFound(derr) {
			return errValue(derr)
		}
		if werr := h.lists().writeList(ctx, o.store, out, 0); werr != nil {
			return errValue(werr)
		}
		return protocol.Value{Kind: protocol.KindInteger, I: int64(len(out))}
	}
	if len(o.gets) == 0 {
		out := make([]protocol.Value, 0, len(items))
		for _, it := range items {
			out = append(out, protocol.BulkOf(it.elem))
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: h.collectGet(ctx, o.gets, items, false)}
}

// collectGet 把 GET 模板展平为存/回数组；缺失在回包中为 nil、在 STORE 中为空串。
func (h *genericHandler) collectGet(ctx context.Context, gets []string, items []sortItem, forStore bool) []protocol.Value {
	out := make([]protocol.Value, 0, len(items)*len(gets))
	for _, it := range items {
		for _, pat := range gets {
			if pat == "#" {
				out = append(out, protocol.BulkOf(it.elem))
				continue
			}
			v, found := h.resolveSortPattern(ctx, pat, it.elem)
			if !found {
				if forStore {
					out = append(out, protocol.BulkOf(""))
					continue
				}
				out = append(out, protocol.Value{Kind: protocol.KindBulkString})
				continue
			}
			out = append(out, protocol.BulkOf(v))
		}
	}
	return out
}
