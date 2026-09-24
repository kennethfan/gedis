package commands

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterZSet 注册 ZSet 核心命令；范围/STORE/阻塞/SCAN 由同包其它文件追加。
func RegisterZSet(r *network.Router, kv KV) {
	h := &zsetHandler{kv: kv}
	r.Register("ZADD", h.zadd)
	r.Register("ZREM", h.zrem)
	r.Register("ZSCORE", h.zscore)
	r.Register("ZMSCORE", h.zmscore)
	r.Register("ZCARD", h.zcard)
	r.Register("ZINCRBY", h.zincrby)
	r.Register("ZRANK", h.zrank)
	r.Register("ZREVRANK", h.zrevrank)
	r.Register("ZCOUNT", h.zcount)
	r.Register("ZPOPMIN", h.zpopmin)
	r.Register("ZPOPMAX", h.zpopmax)
	h.registerRange(r)
	h.registerStore(r)
	h.registerBlockZ(r)
	h.registerScanZ(r)
}

type zsetHandler struct {
	kv KV
}

func (h *zsetHandler) readZSet(ctx context.Context, key string) (map[string]float64, int64, error) {
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil {
		return nil, 0, err
	}
	if e.Type != datastruct.TypeZSet {
		return nil, 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	z, _, err := datastruct.DecodeZSet(e.Payload)
	if err != nil {
		return nil, 0, err
	}
	return z, e.Expiry, nil
}

func (h *zsetHandler) writeZSet(ctx context.Context, key string, z map[string]float64, expiry int64) error {
	if len(z) == 0 {
		return h.kv.Delete(ctx, datastruct.ZSetKey(key))
	}
	return h.kv.Set(ctx, datastruct.ZSetKey(key), datastruct.Encode(datastruct.TypeZSet, expiry, datastruct.EncodeZSet(z)))
}

type zsetMember struct {
	m string
	s float64
}

// sortedZSet 按 (score, member) 升序返回全部成员。
func sortedZSet(z map[string]float64) []zsetMember {
	out := make([]zsetMember, 0, len(z))
	for m, s := range z {
		out = append(out, zsetMember{m: m, s: s})
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && (out[j].s < out[j-1].s || (out[j].s == out[j-1].s && out[j].m < out[j-1].m)); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// formatScore 对标 Redis 的 double 回包：0（含 -0）打印为 0；
// 设 shortest 科学计数 f = d.ddd * 10^E，定点尾零数 Z = E+1-len(数字)：
// E >= 19 且 Z >= 8 用科学计数，E <= -7 用科学计数，其余 shortest 定点。
func formatScore(f float64) string {
	if f == 0 {
		return "0"
	}
	mant, exp := splitSci(f)
	digits := strings.Replace(mant, ".", "", 1)
	if exp >= 19 && exp+1-len(digits) >= 8 {
		return normSci(f)
	}
	if exp <= -7 {
		return normSci(f)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// splitSci 解析 shortest 科学计数，返回尾数与十进制指数 E。
func splitSci(f float64) (string, int) {
	s := strconv.FormatFloat(f, 'e', -1, 64)
	idx := strings.IndexByte(s, 'e')
	n, err := strconv.Atoi(s[idx+1:])
	if err != nil {
		return s, 0
	}
	return s[:idx], n
}

// normSci 输出 shortest 科学计数，指数不补零（1e-07 → 1e-7）。
func normSci(f float64) string {
	s := strconv.FormatFloat(f, 'e', -1, 64)
	s = strings.Replace(s, "e-0", "e-", 1)
	s = strings.Replace(s, "e+0", "e+", 1)
	return s
}

// sciExponent 从 shortest 科学计数中解析十进制指数 E（f = d.ddd * 10^E）。
func sciExponent(f float64) int {
	_, exp := splitSci(f)
	return exp
}

// scoreBound 是 ZRANGEBYSCORE/ZCOUNT 的 min/max 解析结果。
type scoreBound struct {
	v   float64
	ex  bool
	inf int
}

func parseScoreBound(s string) (scoreBound, error) {
	ex := false
	if strings.HasPrefix(s, "(") {
		ex = true
		s = s[1:]
	}
	switch strings.ToLower(s) {
	case "-inf":
		return scoreBound{v: math.Inf(-1), inf: -1}, nil
	case "+inf", "inf":
		return scoreBound{v: math.Inf(1), inf: 1}, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) {
		return scoreBound{}, fmt.Errorf("ERR min or max is not a float")
	}
	return scoreBound{v: f, ex: ex}, nil
}

func inScoreRange(s float64, min, max scoreBound) bool {
	if min.inf == 0 && (s < min.v || (min.ex && s == min.v)) {
		return false
	}
	if max.inf == 0 && (s > max.v || (max.ex && s == max.v)) {
		return false
	}
	return true
}

// normalizeRange 把 Redis 式起止下标（含负数）裁成 [lo,hi]；空返回 ok=false。
func normalizeRange(start, stop, n int64) (lo, hi int64, ok bool) {
	if n == 0 {
		return 0, -1, false
	}
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return 0, -1, false
	}
	return start, stop, true
}

func zsetKey(args []protocol.Value) (string, bool) {
	if len(args) < 1 {
		return "", false
	}
	return argString(args[0])
}

type zaddFlags struct {
	nx, xx, gt, lt, ch, incr bool
}

func parseZAddFlags(args []protocol.Value, from int) (zaddFlags, int, *protocol.Value) {
	var fl zaddFlags
	i := from
	for ; i < len(args); i++ {
		s, ok := argString(args[i])
		if !ok {
			v := errValueStr("ERR invalid argument")
			return fl, i, &v
		}
		switch strings.ToUpper(s) {
		case "NX":
			fl.nx = true
		case "XX":
			fl.xx = true
		case "GT":
			fl.gt = true
		case "LT":
			fl.lt = true
		case "CH":
			fl.ch = true
		case "INCR":
			fl.incr = true
		default:
			return fl, i, nil
		}
	}
	return fl, i, nil
}

func (h *zsetHandler) zadd(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'zadd' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	fl, i, errReply := parseZAddFlags(args, 1)
	if errReply != nil {
		return *errReply
	}
	if fl.gt && fl.lt {
		return errValueStr("ERR syntax error, GT and LT not compatible")
	}
	if (fl.gt || fl.lt) && fl.nx {
		return errValueStr("ERR syntax error, GT, LT, and/or NX options at the same time are not compatible")
	}
	rest := args[i:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return errValueStr("ERR syntax error")
	}
	if fl.incr && len(rest) != 2 {
		return errValueStr("ERR INCR option supports a single increment-element pair")
	}
	z, expiry, err := h.readZSet(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		z = make(map[string]float64)
	}
	var added, changed int64
	for j := 0; j < len(rest); j += 2 {
		sstr, ok := argString(rest[j])
		if !ok {
			return errValueStr("ERR invalid score")
		}
		delta, err := datastruct.ParseScore(sstr)
		if err != nil {
			return errValue(err)
		}
		m, ok := argString(rest[j+1])
		if !ok {
			return errValueStr("ERR invalid member")
		}
		cur, exists := z[m]
		if fl.incr {
			if !exists && fl.xx {
				return protocol.Value{Kind: protocol.KindBulkString}
			}
			if exists && fl.nx {
				return protocol.BulkOf(formatScore(cur))
			}
			cur += delta
			if math.IsNaN(cur) || math.IsInf(cur, 0) {
				return errValueStr("ERR resulting score is out of range (result of INCRBY would be out of range)")
			}
			z[m] = cur
			if werr := h.writeZSet(ctx, key, z, expiry); werr != nil {
				return errValue(werr)
			}
			return protocol.BulkOf(formatScore(cur))
		}
		if !exists {
			if fl.xx {
				continue
			}
			z[m] = delta
			added++
			changed++
			continue
		}
		if fl.nx {
			continue
		}
		if fl.gt && delta <= cur {
			continue
		}
		if fl.lt && delta >= cur {
			continue
		}
		if delta != cur {
			z[m] = delta
			changed++
		}
	}
	if werr := h.writeZSet(ctx, key, z, expiry); werr != nil {
		return errValue(werr)
	}
	if fl.ch {
		return protocol.Value{Kind: protocol.KindInteger, I: changed}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: added}
}

func (h *zsetHandler) zrem(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'zrem' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	z, expiry, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	var removed int64
	for _, a := range args[1:] {
		m, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid member")
		}
		if _, exists := z[m]; exists {
			delete(z, m)
			removed++
		}
	}
	if werr := h.writeZSet(ctx, key, z, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: removed}
}

func (h *zsetHandler) zscore(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'zscore' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	z, _, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	m, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid member")
	}
	s, exists := z[m]
	if !exists {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	return protocol.BulkOf(formatScore(s))
}

func (h *zsetHandler) zmscore(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'zmscore' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	z, _, err := h.readZSet(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		z = nil
	}
	out := make([]protocol.Value, 0, len(args)-1)
	for _, a := range args[1:] {
		m, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid member")
		}
		if s, exists := z[m]; exists {
			out = append(out, protocol.BulkOf(formatScore(s)))
		} else {
			out = append(out, protocol.Value{Kind: protocol.KindBulkString})
		}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *zsetHandler) zcard(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'zcard' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	z, _, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(z))}
}

func (h *zsetHandler) zincrby(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'zincrby' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	incrStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid increment")
	}
	incr, err := datastruct.ParseScore(incrStr)
	if err != nil {
		return errValue(err)
	}
	m, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid member")
	}
	z, expiry, err := h.readZSet(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		z = make(map[string]float64)
	}
	ns := z[m] + incr
	if math.IsNaN(ns) || math.IsInf(ns, 0) {
		return errValueStr("ERR resulting score is out of range (result of INCRBY would be out of range)")
	}
	z[m] = ns
	if werr := h.writeZSet(ctx, key, z, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.BulkOf(formatScore(ns))
}

// rankOf 返回 member 在升序中的下标；rev=true 时返回降序下标。
func rankOf(z map[string]float64, member string, rev bool) (int64, float64, bool) {
	sorted := sortedZSet(z)
	for i, p := range sorted {
		if p.m == member {
			r := int64(i)
			if rev {
				r = int64(len(sorted) - 1 - i)
			}
			return r, p.s, true
		}
	}
	return 0, 0, false
}

func (h *zsetHandler) zrank(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.rank(ctx, args, false, "zrank")
}

func (h *zsetHandler) zrevrank(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.rank(ctx, args, true, "zrevrank")
}

func (h *zsetHandler) rank(ctx context.Context, args []protocol.Value, rev bool, name string) protocol.Value {
	if len(args) < 2 || len(args) > 3 {
		return errValueStr("ERR wrong number of arguments for '" + name + "' command")
	}
	withScore := false
	if len(args) == 3 {
		opt, ok := argString(args[2])
		if !ok || !strings.EqualFold(opt, "WITHSCORE") {
			return errValueStr("ERR syntax error")
		}
		withScore = true
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	z, _, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	m, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid member")
	}
	r, s, exists := rankOf(z, m, rev)
	if !exists {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	if withScore {
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			{Kind: protocol.KindInteger, I: r},
			protocol.BulkOf(formatScore(s)),
		}}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: r}
}

func (h *zsetHandler) zcount(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'zcount' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	minStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid min")
	}
	maxStr, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid max")
	}
	min, err := parseScoreBound(minStr)
	if err != nil {
		return errValue(err)
	}
	max, err := parseScoreBound(maxStr)
	if err != nil {
		return errValue(err)
	}
	z, _, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	var n int64
	for _, s := range z {
		if inScoreRange(s, min, max) {
			n++
		}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: n}
}

func (h *zsetHandler) zpopmin(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.zpop(ctx, args, false, "zpopmin")
}

func (h *zsetHandler) zpopmax(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.zpop(ctx, args, true, "zpopmax")
}

func (h *zsetHandler) zpop(ctx context.Context, args []protocol.Value, rev bool, name string) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return errValueStr("ERR wrong number of arguments for '" + name + "' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	var count int64 = 1
	if len(args) == 2 {
		cstr, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR invalid count")
		}
		count, ok = parseCount(cstr)
		if !ok || count < 0 {
			return errValueStr("ERR value is out of range, must be positive")
		}
	}
	z, expiry, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
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
	out := make([]protocol.Value, 0, count*2)
	for i := int64(0); i < count; i++ {
		out = append(out, protocol.BulkOf(sorted[i].m), protocol.BulkOf(formatScore(sorted[i].s)))
		delete(z, sorted[i].m)
	}
	if werr := h.writeZSet(ctx, key, z, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func parseCount(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
