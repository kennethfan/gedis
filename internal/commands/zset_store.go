package commands

import (
	"context"
	"strings"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
)

func (h *zsetHandler) registerStore(r *network.Router) {
	r.Register("ZUNION", h.zunion)
	r.Register("ZINTER", h.zinter)
	r.Register("ZDIFF", h.zdiff)
	r.Register("ZUNIONSTORE", h.zunionstore)
	r.Register("ZINTERSTORE", h.zinterstore)
	r.Register("ZDIFFSTORE", h.zdiffstore)
}

type zsetAggregate int

const (
	aggSum zsetAggregate = iota
	aggMin
	aggMax
)

type zsetOpSpec struct {
	keys       []string
	weights    []float64
	agg        zsetAggregate
	withScores bool
}

// parseZSetOp 解析 [numkeys key...] [WEIGHTS w...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]。
// allowExtra=false 时拒绝 WEIGHTS/AGGREGATE/WITHSCORES（ZDIFF* 用）。
func parseZSetOp(args []protocol.Value, name string, allowExtra bool) (zsetOpSpec, *protocol.Value) {
	var spec zsetOpSpec
	if len(args) < 1 {
		v := errValueStr("ERR wrong number of arguments for '" + name + "' command")
		return spec, &v
	}
	numStr, ok := argString(args[0])
	if !ok {
		v := errValueStr("ERR invalid numkeys")
		return spec, &v
	}
	num, ok := parseCount(numStr)
	if !ok || num < 1 {
		v := errValueStr("ERR at least 1 input key is needed for '" + name + "' command")
		return spec, &v
	}
	if int64(len(args)) < 1+num {
		v := errValueStr("ERR wrong number of arguments for '" + name + "' command")
		return spec, &v
	}
	spec.keys = make([]string, 0, num)
	for _, a := range args[1 : 1+num] {
		k, ok := argString(a)
		if !ok {
			v := errValueStr("ERR invalid key")
			return spec, &v
		}
		spec.keys = append(spec.keys, k)
	}
	i := 1 + int(num)
	for ; i < len(args); i++ {
		opt, ok := argString(args[i])
		if !ok {
			v := errValueStr("ERR syntax error")
			return spec, &v
		}
		switch strings.ToUpper(opt) {
		case "WEIGHTS":
			if !allowExtra {
				v := errValueStr("ERR syntax error")
				return spec, &v
			}
			if int64(len(args)) < int64(i)+1+num {
				v := errValueStr("ERR syntax error")
				return spec, &v
			}
			spec.weights = make([]float64, 0, num)
			for _, a := range args[i+1 : i+1+int(num)] {
				wstr, ok := argString(a)
				if !ok {
					v := errValueStr("ERR invalid weight")
					return spec, &v
				}
				w, err := datastruct.ParseScore(wstr)
				if err != nil {
					v := errValueStr("ERR weight value is not a float")
					return spec, &v
				}
				spec.weights = append(spec.weights, w)
			}
			i += int(num)
		case "AGGREGATE":
			if !allowExtra {
				v := errValueStr("ERR syntax error")
				return spec, &v
			}
			if i+1 >= len(args) {
				v := errValueStr("ERR syntax error")
				return spec, &v
			}
			aggStr, ok := argString(args[i+1])
			if !ok {
				v := errValueStr("ERR syntax error")
				return spec, &v
			}
			switch strings.ToUpper(aggStr) {
			case "SUM":
				spec.agg = aggSum
			case "MIN":
				spec.agg = aggMin
			case "MAX":
				spec.agg = aggMax
			default:
				v := errValueStr("ERR syntax error")
				return spec, &v
			}
			i++
		case "WITHSCORES":
			if !allowExtra {
				v := errValueStr("ERR syntax error")
				return spec, &v
			}
			spec.withScores = true
		default:
			v := errValueStr("ERR syntax error")
			return spec, &v
		}
	}
	if spec.weights == nil {
		spec.weights = make([]float64, len(spec.keys))
		for i := range spec.weights {
			spec.weights[i] = 1
		}
	}
	return spec, nil
}

// readZSets 读多个 key：缺失视为空集；任一类型错误返回 WRONGTYPE。
func (h *zsetHandler) readZSets(ctx context.Context, keys []string) ([]map[string]float64, *protocol.Value) {
	sets := make([]map[string]float64, 0, len(keys))
	for _, k := range keys {
		z, _, err := h.readZSet(ctx, k)
		if err != nil {
			if !isNotFound(err) {
				v := errValue(err)
				return nil, &v
			}
			z = make(map[string]float64)
		}
		sets = append(sets, z)
	}
	return sets, nil
}

func aggregateScore(agg zsetAggregate, cur float64, hasCur bool, next float64) float64 {
	if !hasCur {
		return next
	}
	switch agg {
	case aggMin:
		if next < cur {
			return next
		}
		return cur
	case aggMax:
		if next > cur {
			return next
		}
		return cur
	default:
		return cur + next
	}
}

func unionZSets(sets []map[string]float64, weights []float64, agg zsetAggregate) map[string]float64 {
	out := make(map[string]float64)
	seen := make(map[string]bool)
	for i, z := range sets {
		for m, s := range z {
			ws := s * weights[i]
			out[m] = aggregateScore(agg, out[m], seen[m], ws)
			seen[m] = true
		}
	}
	return out
}

func interZSets(sets []map[string]float64, weights []float64, agg zsetAggregate) map[string]float64 {
	out := make(map[string]float64)
	for m, s := range sets[0] {
		acc := s * weights[0]
		inAll := true
		for i, z := range sets[1:] {
			s2, ok := z[m]
			if !ok {
				inAll = false
				break
			}
			acc = aggregateScore(agg, acc, true, s2*weights[i+1])
		}
		if !inAll {
			continue
		}
		out[m] = acc
	}
	return out
}

func diffZSets(sets []map[string]float64) map[string]float64 {
	out := make(map[string]float64)
	for m, s := range sets[0] {
		excluded := false
		for _, z := range sets[1:] {
			if _, ok := z[m]; ok {
				excluded = true
				break
			}
		}
		if !excluded {
			out[m] = s
		}
	}
	return out
}

func (h *zsetHandler) zunion(ctx context.Context, args []protocol.Value) protocol.Value {
	spec, errReply := parseZSetOp(args, "zunion", true)
	if errReply != nil {
		return *errReply
	}
	sets, errReply := h.readZSets(ctx, spec.keys)
	if errReply != nil {
		return *errReply
	}
	res := unionZSets(sets, spec.weights, spec.agg)
	return rangeReply(sortedZSet(res), spec.withScores)
}

func (h *zsetHandler) zinter(ctx context.Context, args []protocol.Value) protocol.Value {
	spec, errReply := parseZSetOp(args, "zinter", true)
	if errReply != nil {
		return *errReply
	}
	sets, errReply := h.readZSets(ctx, spec.keys)
	if errReply != nil {
		return *errReply
	}
	res := interZSets(sets, spec.weights, spec.agg)
	return rangeReply(sortedZSet(res), spec.withScores)
}

func (h *zsetHandler) zdiff(ctx context.Context, args []protocol.Value) protocol.Value {
	spec, errReply := parseZSetOp(args, "zdiff", false)
	if errReply != nil {
		return *errReply
	}
	sets, errReply := h.readZSets(ctx, spec.keys)
	if errReply != nil {
		return *errReply
	}
	return rangeReply(sortedZSet(diffZSets(sets)), false)
}

type zsetOpFunc func([]map[string]float64, []float64, zsetAggregate) map[string]float64

func unionOp(sets []map[string]float64, w []float64, a zsetAggregate) map[string]float64 {
	return unionZSets(sets, w, a)
}

func interOp(sets []map[string]float64, w []float64, a zsetAggregate) map[string]float64 {
	return interZSets(sets, w, a)
}

// storeZSetOp 是 STORE 三件套公共路径：目标类型先验 WRONGTYPE，再 WriteBatch 原子落盘。
func (h *zsetHandler) storeZSetOp(ctx context.Context, args []protocol.Value, name string, op zsetOpFunc, allowExtra bool) protocol.Value {
	if len(args) < 2 {
		v := errValueStr("ERR wrong number of arguments for '" + name + "' command")
		return v
	}
	dst, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid destination")
	}
	spec, errReply := parseZSetOp(args[1:], name, allowExtra)
	if errReply != nil {
		return *errReply
	}
	if _, _, err := h.readZSet(ctx, dst); err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
	}
	sets, errReply := h.readZSets(ctx, spec.keys)
	if errReply != nil {
		return *errReply
	}
	res := op(sets, spec.weights, spec.agg)
	var ops []storage.BatchOp
	if len(res) == 0 {
		ops = []storage.BatchOp{{Key: datastruct.ZSetKey(dst), Delete: true}}
	} else {
		ops = []storage.BatchOp{{
			Key:   datastruct.ZSetKey(dst),
			Value: datastruct.Encode(datastruct.TypeZSet, 0, datastruct.EncodeZSet(res)),
		}}
	}
	if err := h.kv.WriteBatch(ctx, ops); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(res))}
}

func (h *zsetHandler) zunionstore(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.storeZSetOp(ctx, args, "zunionstore", unionOp, true)
}

func (h *zsetHandler) zinterstore(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.storeZSetOp(ctx, args, "zinterstore", interOp, true)
}

func (h *zsetHandler) zdiffstore(ctx context.Context, args []protocol.Value) protocol.Value {
	return h.storeZSetOp(ctx, args, "zdiffstore", func(sets []map[string]float64, _ []float64, _ zsetAggregate) map[string]float64 {
		return diffZSets(sets)
	}, false)
}
