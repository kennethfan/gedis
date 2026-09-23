package commands

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func (h *zsetHandler) registerRange(r *network.Router) {
	r.Register("ZRANGE", h.zrange)
	r.Register("ZREVRANGE", h.zrevrange)
	r.Register("ZRANGEBYSCORE", h.zrangebyscore)
	r.Register("ZREVRANGEBYSCORE", h.zrevrangebyscore)
	r.Register("ZRANGEBYLEX", h.zrangebylex)
	r.Register("ZREVRANGEBYLEX", h.zrevrangebylex)
	r.Register("ZREMRANGEBYRANK", h.zremrangebyrank)
	r.Register("ZREMRANGEBYSCORE", h.zremrangebyscore)
	r.Register("ZREMRANGEBYLEX", h.zremrangebylex)
	r.Register("ZRANGESTORE", h.zrangestore)
}

// lexBound 是 BYLEX 的 min/max 解析结果。
type lexBound struct {
	v   string
	ex  bool
	inf int
}

func parseLexBound(s string) (lexBound, error) {
	switch s {
	case "-":
		return lexBound{inf: -1}, nil
	case "+":
		return lexBound{inf: 1}, nil
	}
	if strings.HasPrefix(s, "[") {
		return lexBound{v: s[1:]}, nil
	}
	if strings.HasPrefix(s, "(") {
		return lexBound{v: s[1:], ex: true}, nil
	}
	return lexBound{}, fmt.Errorf("ERR min or max not valid string range item")
}

func inLexRange(m string, min, max lexBound) bool {
	if min.inf == 0 {
		if c := strings.Compare(m, min.v); c < 0 || (min.ex && c == 0) {
			return false
		}
	}
	if max.inf == 0 {
		if c := strings.Compare(m, max.v); c > 0 || (max.ex && c == 0) {
			return false
		}
	}
	return true
}

func lexRangeMembers(sorted []zsetMember, min, max lexBound, rev bool) []zsetMember {
	n := len(sorted)
	if n == 0 {
		return []zsetMember{}
	}
	var out []zsetMember
	if !rev {
		start := 0
		if min.inf == 0 {
			start = n
			for i, e := range sorted {
				if c := strings.Compare(e.m, min.v); c > 0 || (!min.ex && c == 0) {
					start = i
					break
				}
			}
		}
		for i := start; i < n; i++ {
			if max.inf == 0 {
				if c := strings.Compare(sorted[i].m, max.v); c > 0 || (max.ex && c == 0) {
					break
				}
			}
			out = append(out, sorted[i])
		}
		return out
	}
	start := n - 1
	if max.inf == 0 {
		start = -1
		for i := n - 1; i >= 0; i-- {
			if c := strings.Compare(sorted[i].m, max.v); c < 0 || (!max.ex && c == 0) {
				start = i
				break
			}
		}
	}
	for i := start; i >= 0; i-- {
		if min.inf == 0 {
			if c := strings.Compare(sorted[i].m, min.v); c < 0 || (min.ex && c == 0) {
				break
			}
		}
		out = append(out, sorted[i])
	}
	return out
}
type rangeQuery struct {
	byScore, byLex, rev, withScores bool
	start, stop                     int64
	hasRank                         bool
	min, max                        string
	limitOffset, limitCount          int64
	hasLimit                        bool
}

func parseRangeQuery(args []protocol.Value, name string) (key string, q rangeQuery, errReply *protocol.Value) {
	if len(args) < 3 {
		v := errValueStr("ERR wrong number of arguments for '" + name + "' command")
		return "", q, &v
	}
	var ok bool
	key, ok = argString(args[0])
	if !ok {
		v := errValueStr("ERR invalid key")
		return "", q, &v
	}
	minOrStart, ok := argString(args[1])
	if !ok {
		v := errValueStr("ERR invalid start")
		return "", q, &v
	}
	maxOrStop, ok := argString(args[2])
	if !ok {
		v := errValueStr("ERR invalid stop")
		return "", q, &v
	}
	i := 3
	for ; i < len(args); i++ {
		opt, ok := argString(args[i])
		if !ok {
			v := errValueStr("ERR syntax error")
			return "", q, &v
		}
		switch strings.ToUpper(opt) {
		case "BYSCORE":
			q.byScore = true
		case "BYLEX":
			q.byLex = true
		case "REV":
			q.rev = true
		case "WITHSCORES":
			q.withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				v := errValueStr("ERR syntax error")
				return "", q, &v
			}
			off, ok1 := argString(args[i+1])
			cnt, ok2 := argString(args[i+2])
			if !ok1 || !ok2 {
				v := errValueStr("ERR syntax error")
				return "", q, &v
			}
			o, e1 := strconv.ParseInt(off, 10, 64)
			c, e2 := strconv.ParseInt(cnt, 10, 64)
			if e1 != nil || e2 != nil {
				v := errValueStr("ERR value is not an integer or out of range")
				return "", q, &v
			}
			q.hasLimit = true
			q.limitOffset, q.limitCount = o, c
			i += 2
		default:
			v := errValueStr("ERR syntax error")
			return "", q, &v
		}
	}
	if q.byScore && q.byLex {
		v := errValueStr("ERR syntax error, BYSCORE and BYLEX are incompatible")
		return "", q, &v
	}
	if q.byScore || q.byLex {
		q.min, q.max = minOrStart, maxOrStop
	} else {
		s, e1 := strconv.ParseInt(minOrStart, 10, 64)
		e, e2 := strconv.ParseInt(maxOrStop, 10, 64)
		if e1 != nil || e2 != nil {
			v := errValueStr("ERR value is not an integer or out of range")
			return "", q, &v
		}
		q.hasRank, q.start, q.stop = true, s, e
		if q.hasLimit {
			v := errValueStr("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
			return "", q, &v
		}
	}
	// REV 与 Redis 对齐：交换 min max 再反向遍历。
	if q.rev && (q.byScore || q.byLex) {
		q.min, q.max = q.max, q.min
	}
	return key, q, nil
}

// evalRange 对有序成员执行查询，返回命中（已按最终顺序排好）。
func evalRange(sorted []zsetMember, q rangeQuery) []zsetMember {
	out := sorted
	if q.byScore {
		min, err1 := parseScoreBound(q.min)
		max, err2 := parseScoreBound(q.max)
		if err1 != nil || err2 != nil {
			return nil
		}
		filtered := out[:0:0]
		for _, p := range out {
			if inScoreRange(p.s, min, max) {
				filtered = append(filtered, p)
			}
		}
		out = filtered
	} else if q.byLex {
		min, err1 := parseLexBound(q.min)
		max, err2 := parseLexBound(q.max)
		if err1 != nil || err2 != nil {
			return nil
		}
		out = lexRangeMembers(out, min, max, q.rev)
	}
	if q.rev && !q.byLex {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	if q.hasLimit {
		if q.limitOffset < 0 {
			q.limitOffset = 0
		}
		if q.limitOffset >= int64(len(out)) || q.limitCount < 0 {
			return []zsetMember{}
		}
		end := q.limitOffset + q.limitCount
		if q.limitCount < 0 || end > int64(len(out)) {
			end = int64(len(out))
		}
		out = out[q.limitOffset:end]
		return out
	}
	if q.hasRank {
		lo, hi, ok := normalizeRange(q.start, q.stop, int64(len(out)))
		if !ok {
			return []zsetMember{}
		}
		return out[lo : hi+1]
	}
	return out
}

func rangeReply(members []zsetMember, withScores bool) protocol.Value {
	out := make([]protocol.Value, 0, len(members))
	for _, p := range members {
		out = append(out, protocol.BulkOf(p.m))
		if withScores {
			out = append(out, protocol.BulkOf(formatScore(p.s)))
		}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *zsetHandler) zrange(ctx context.Context, args []protocol.Value) protocol.Value {
	key, q, errReply := parseRangeQuery(args, "zrange")
	if errReply != nil {
		return *errReply
	}
	if q.byScore {
		if _, err := parseScoreBound(q.min); err != nil {
			return errValue(err)
		}
		if _, err := parseScoreBound(q.max); err != nil {
			return errValue(err)
		}
	}
	if q.byLex {
		if _, err := parseLexBound(q.min); err != nil {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		if _, err := parseLexBound(q.max); err != nil {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
	}
	z, _, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	return rangeReply(evalRange(sortedZSet(z), q), q.withScores)
}

func (h *zsetHandler) zrevrange(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 || len(args) > 4 {
		return errValueStr("ERR wrong number of arguments for 'zrevrange' command")
	}
	withScores := false
	if len(args) == 4 {
		opt, ok := argString(args[3])
		if !ok || !strings.EqualFold(opt, "WITHSCORES") {
			return errValueStr("ERR syntax error")
		}
		withScores = true
	}
	newArgs := append([]protocol.Value{args[0], args[1], args[2], protocol.BulkOf("REV")}, []protocol.Value{}...)
	if withScores {
		newArgs = append(newArgs, protocol.BulkOf("WITHSCORES"))
	}
	key, q, errReply := parseRangeQuery(newArgs, "zrevrange")
	if errReply != nil {
		return *errReply
	}
	z, _, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	return rangeReply(evalRange(sortedZSet(z), q), withScores)
}

func scoreRangeArgs(key string, args []protocol.Value, name string, rev bool) (string, rangeQuery, *protocol.Value) {
	full := []protocol.Value{protocol.BulkOf(key)}
	full = append(full, args...)
	full = append(full, protocol.BulkOf("BYSCORE"))
	if rev {
		full = append(full, protocol.BulkOf("REV"))
	}
	k, q, errReply := parseRangeQuery(full, name)
	return k, q, errReply
}

func (h *zsetHandler) zrangebyscore(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'zrangebyscore' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	rest := append([]protocol.Value{}, args[1:]...)
	withScores := false
	kept := make([]protocol.Value, 0, len(rest))
	for i := 0; i < len(rest); i++ {
		opt, ok := argString(rest[i])
		if ok && strings.EqualFold(opt, "WITHSCORES") {
			withScores = true
			continue
		}
		kept = append(kept, rest[i])
	}
	k, q, errReply := scoreRangeArgs(key, kept, "zrangebyscore", false)
	if errReply != nil {
		return *errReply
	}
	q.withScores = withScores
	if _, err := parseScoreBound(q.min); err != nil {
		return errValue(err)
	}
	if _, err := parseScoreBound(q.max); err != nil {
		return errValue(err)
	}
	z, _, err := h.readZSet(ctx, k)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	return rangeReply(evalRange(sortedZSet(z), q), withScores)
}

func (h *zsetHandler) zrevrangebyscore(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'zrevrangebyscore' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	kept := make([]protocol.Value, 0, len(args)-1)
	withScores := false
	for i := 1; i < len(args); i++ {
		opt, ok := argString(args[i])
		if ok && strings.EqualFold(opt, "WITHSCORES") {
			withScores = true
			continue
		}
		kept = append(kept, args[i])
	}
	// ZREVRANGEBYSCORE key max min：交换由 parseRangeQuery 按 REV 统一做。
	k, q, errReply := scoreRangeArgs(key, kept, "zrevrangebyscore", true)
	if errReply != nil {
		return *errReply
	}
	q.withScores = withScores
	if _, err := parseScoreBound(q.min); err != nil {
		return errValue(err)
	}
	if _, err := parseScoreBound(q.max); err != nil {
		return errValue(err)
	}
	z, _, err := h.readZSet(ctx, k)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	return rangeReply(evalRange(sortedZSet(z), q), withScores)
}

func lexRangeArgs(key string, args []protocol.Value, name string, rev bool) (string, rangeQuery, *protocol.Value) {
	full := []protocol.Value{protocol.BulkOf(key)}
	full = append(full, args...)
	full = append(full, protocol.BulkOf("BYLEX"))
	if rev {
		full = append(full, protocol.BulkOf("REV"))
	}
	return parseRangeQuery(full, name)
}

func (h *zsetHandler) zrangebylex(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'zrangebylex' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	k, q, errReply := lexRangeArgs(key, args[1:], "zrangebylex", false)
	if errReply != nil {
		return *errReply
	}
	if _, err := parseLexBound(q.min); err != nil {
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	}
	if _, err := parseLexBound(q.max); err != nil {
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	}
	z, _, err := h.readZSet(ctx, k)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	return rangeReply(evalRange(sortedZSet(z), q), false)
}

func (h *zsetHandler) zrevrangebylex(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'zrevrangebylex' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	kept := append([]protocol.Value{}, args[1:]...)
	k, q, errReply := lexRangeArgs(key, kept, "zrevrangebylex", true)
	if errReply != nil {
		return *errReply
	}
	if _, err := parseLexBound(q.min); err != nil {
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	}
	if _, err := parseLexBound(q.max); err != nil {
		return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
	}
	z, _, err := h.readZSet(ctx, k)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
		}
		return errValue(err)
	}
	return rangeReply(evalRange(sortedZSet(z), q), false)
}

func (h *zsetHandler) zremrangebyrank(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'zremrangebyrank' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	startStr, _ := argString(args[1])
	stopStr, _ := argString(args[2])
	start, e1 := strconv.ParseInt(startStr, 10, 64)
	stop, e2 := strconv.ParseInt(stopStr, 10, 64)
	if e1 != nil || e2 != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	z, expiry, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	sorted := sortedZSet(z)
	lo, hi, ok := normalizeRange(start, stop, int64(len(sorted)))
	if !ok {
		return protocol.Value{Kind: protocol.KindInteger}
	}
	for _, p := range sorted[lo : hi+1] {
		delete(z, p.m)
	}
	if werr := h.writeZSet(ctx, key, z, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: hi - lo + 1}
}

func (h *zsetHandler) zremrangebyscore(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'zremrangebyscore' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	minStr, _ := argString(args[1])
	maxStr, _ := argString(args[2])
	min, err := parseScoreBound(minStr)
	if err != nil {
		return errValue(err)
	}
	max, err := parseScoreBound(maxStr)
	if err != nil {
		return errValue(err)
	}
	z, expiry, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	var n int64
	for m, s := range z {
		if inScoreRange(s, min, max) {
			delete(z, m)
			n++
		}
	}
	if werr := h.writeZSet(ctx, key, z, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: n}
}

func (h *zsetHandler) zremrangebylex(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'zremrangebylex' command")
	}
	key, ok := zsetKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	minStr, _ := argString(args[1])
	maxStr, _ := argString(args[2])
	min, err := parseLexBound(minStr)
	if err != nil {
		return protocol.Value{Kind: protocol.KindInteger}
	}
	max, err := parseLexBound(maxStr)
	if err != nil {
		return protocol.Value{Kind: protocol.KindInteger}
	}
	z, expiry, err := h.readZSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		return errValue(err)
	}
	inRange := lexRangeMembers(sortedZSet(z), min, max, false)
	var n int64
	for _, e := range inRange {
		if _, ok := z[e.m]; ok {
			delete(z, e.m)
			n++
		}
	}
	if werr := h.writeZSet(ctx, key, z, expiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: n}
}

func (h *zsetHandler) zrangestore(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 4 {
		return errValueStr("ERR wrong number of arguments for 'zrangestore' command")
	}
	dst, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	rewritten := append([]protocol.Value{args[1], args[2], args[3]}, args[4:]...)
	src, q, errReply := parseRangeQuery(rewritten, "zrangestore")
	if errReply != nil {
		return *errReply
	}
	if q.withScores {
		return errValueStr("ERR syntax error")
	}
	if q.byScore {
		if _, err := parseScoreBound(q.min); err != nil {
			return errValue(err)
		}
		if _, err := parseScoreBound(q.max); err != nil {
			return errValue(err)
		}
	}
	if q.byLex {
		if _, err := parseLexBound(q.min); err != nil {
			return protocol.Value{Kind: protocol.KindInteger}
		}
		if _, err := parseLexBound(q.max); err != nil {
			return protocol.Value{Kind: protocol.KindInteger}
		}
	}
	z, _, err := h.readZSet(ctx, src)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		_ = h.writeZSet(ctx, dst, map[string]float64{}, 0)
		return protocol.Value{Kind: protocol.KindInteger}
	}
	hit := evalRange(sortedZSet(z), q)
	dstZ := make(map[string]float64, len(hit))
	for _, p := range hit {
		dstZ[p.m] = p.s
	}
	// 目标 key 若为非 zset 类型必须报 WRONGTYPE（先探测）。
	if _, _, err := h.readZSet(ctx, dst); err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
	}
	if werr := h.writeZSet(ctx, dst, dstZ, 0); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(hit))}
}
