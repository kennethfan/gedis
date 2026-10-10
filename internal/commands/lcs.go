package commands

import (
	"context"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

var lcsMeta = []acl.Meta{
	{Name: "LCS", Category: "string", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 1}},
}

func (s *stringHandler) registerLCS(r *network.Router) {
	for _, m := range lcsMeta {
		acl.RegisterMeta(m)
	}
	r.Register("LCS", s.lcs)
}

// lcsMaxCells 是 DP 表上限（int32 单元）：超限报 ERR 而非 OOM。
const lcsMaxCells = int64(8 << 20)

func (s *stringHandler) lcs(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'lcs' command")
	}
	aKey, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	bKey, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	var wantLen, wantIdx, withMatchLen bool
	var minMatchLen int64
	for i := 2; i < len(args); {
		opt, ok := argString(args[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(opt) {
		case "LEN":
			wantLen = true
			i++
		case "IDX":
			wantIdx = true
			i++
		case "WITHMATCHLEN":
			withMatchLen = true
			i++
		case "MINMATCHLEN":
			if i+1 >= len(args) {
				return errValueStr("ERR syntax error")
			}
			vstr, ok := argString(args[i+1])
			if !ok {
				return errValueStr("ERR value is not an integer or out of range")
			}
			n, err := strconv.ParseInt(vstr, 10, 64)
			if err != nil {
				return errValueStr("ERR value is not an integer or out of range")
			}
			minMatchLen = n
			i += 2
		default:
			return errValueStr("ERR syntax error")
		}
	}
	if wantLen && wantIdx {
		return errValueStr("ERR If you want both the length and indexes, please just use IDX.")
	}
	a, ev, done := s.lcsString(ctx, aKey)
	if done {
		return ev
	}
	b, ev, done := s.lcsString(ctx, bKey)
	if done {
		return ev
	}
	tab, l := lcsTable(a, b)
	if tab == nil {
		return errValueStr("ERR LCS strings too long")
	}
	if wantLen {
		return protocol.Value{Kind: protocol.KindInteger, I: int64(l)}
	}
	pairs := lcsBacktrack(a, b, tab)
	if !wantIdx {
		out := make([]byte, 0, l)
		for i := len(pairs) - 1; i >= 0; i-- {
			out = append(out, a[pairs[i][0]])
		}
		return protocol.Value{Kind: protocol.KindBulkString, Bulk: out}
	}
	matches := lcsRanges(pairs, minMatchLen, withMatchLen)
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("matches"),
		{Kind: protocol.KindArray, Elems: matches},
		protocol.BulkOf("len"),
		{Kind: protocol.KindInteger, I: int64(l)},
	}}
}

// lcsString 取串值：缺 key 视空串；非串类型回 LCS 专属 wrongtype 文案。
func (s *stringHandler) lcsString(ctx context.Context, key string) (string, protocol.Value, bool) {
	e, err := s.getAny(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return "", protocol.Value{}, false
		}
		return "", errValue(err), true
	}
	if e.Type != datastruct.TypeString {
		return "", errValueStr("ERR The specified keys must contain string values"), true
	}
	return string(e.Payload), protocol.Value{}, false
}

// lcsTable 建 (n+1)*(m+1) DP 表；超限回 nil。
func lcsTable(a, b string) ([]int32, int32) {
	n, m := len(a), len(b)
	if int64(n+1)*int64(m+1) > lcsMaxCells {
		return nil, 0
	}
	tab := make([]int32, (n+1)*(m+1))
	stride := m + 1
	for i := 1; i <= n; i++ {
		for j := 1; j <= m; j++ {
			if a[i-1] == b[j-1] {
				tab[i*stride+j] = tab[(i-1)*stride+j-1] + 1
			} else if tab[(i-1)*stride+j] >= tab[i*stride+j-1] {
				tab[i*stride+j] = tab[(i-1)*stride+j]
			} else {
				tab[i*stride+j] = tab[i*stride+j-1]
			}
		}
	}
	return tab, tab[n*stride+m]
}

// lcsBacktrack 回溯对齐下标（降序：与真机 matches 顺序一致）。
func lcsBacktrack(a, b string, tab []int32) [][2]int {
	n, m := len(a), len(b)
	stride := m + 1
	var pairs [][2]int
	for i, j := n, m; i > 0 && j > 0; {
		if a[i-1] == b[j-1] {
			pairs = append(pairs, [2]int{i - 1, j - 1})
			i--
			j--
		} else if tab[(i-1)*stride+j] >= tab[i*stride+j-1] {
			i--
		} else {
			j--
		}
	}
	return pairs
}

// lcsRanges 把降序对齐点并成连续区间并按 MINMATCHLEN 过滤。
func lcsRanges(pairs [][2]int, minMatchLen int64, withMatchLen bool) []protocol.Value {
	intVal := func(v int) protocol.Value { return protocol.Value{Kind: protocol.KindInteger, I: int64(v)} }
	var out []protocol.Value
	flush := func(a0, a1, b0, b1 int) {
		if int64(a1-a0+1) < minMatchLen {
			return
		}
		mv := []protocol.Value{
			{Kind: protocol.KindArray, Elems: []protocol.Value{intVal(a0), intVal(a1)}},
			{Kind: protocol.KindArray, Elems: []protocol.Value{intVal(b0), intVal(b1)}},
		}
		if withMatchLen {
			mv = append(mv, intVal(a1-a0+1))
		}
		out = append(out, protocol.Value{Kind: protocol.KindArray, Elems: mv})
	}
	for k := 0; k < len(pairs); {
		a1, b1 := pairs[k][0], pairs[k][1]
		a0, b0 := a1, b1
		for k+1 < len(pairs) && pairs[k+1][0] == a0-1 && pairs[k+1][1] == b0-1 {
			k++
			a0, b0 = pairs[k][0], pairs[k][1]
		}
		flush(a0, a1, b0, b1)
		k++
	}
	if out == nil {
		out = []protocol.Value{}
	}
	return out
}
