package commands

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterVector 注册 P0 vector set 子集。编码细节见 datastruct/vector.go。
//
// P0 偏差（相对真机 8.0，文档声明）：只收 FP32/VALUES 输入形态；量化/REDUCE/
// CAS/EF/M 等选项遇即 syntax error；VSIM 为暴力精确扫描（恒等 TRUTH 语义）。
var vectorMeta = []acl.Meta{
	{Name: "VADD", Category: "vectorset", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "VCARD", Category: "vectorset", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "VDIM", Category: "vectorset", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "VEMB", Category: "vectorset", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "VREM", Category: "vectorset", Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "VRANDMEMBER", Category: "vectorset", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "VSIM", Category: "vectorset", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
}

func RegisterVector(r *network.Router, kv KV) {
	for _, m := range vectorMeta {
		acl.RegisterMeta(m)
	}
	h := &vectorHandler{kv: kv}
	r.Register("VADD", h.vadd)
	r.Register("VCARD", h.vcard)
	r.Register("VDIM", h.vdim)
	r.Register("VEMB", h.vemb)
	r.Register("VREM", h.vrem)
	r.Register("VRANDMEMBER", h.vrandmember)
	r.Register("VSIM", h.vsim)
}

type vectorHandler struct {
	kv KV
}

// errVectorType WRONGTYPE 文案照真机 vectorset 口径。
func errVectorType() protocol.Value {
	return errValueStr("WRONGTYPE Key is not a valid vectorset value.")
}

// loadVector 读整对象：lookupKey 跨前缀探测（命中别类型即 WRONGTYPE，
// 顺带处理过期）；缺键回 (nil,false,零值)；解码失败按 WRONGTYPE 计。
func (h *vectorHandler) loadVector(ctx context.Context, key string) (map[string]datastruct.VEntry, bool, protocol.Value) {
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil {
		if isNotFound(err) {
			return nil, false, protocol.Value{}
		}
		return nil, false, errValue(err)
	}
	if e.Type != datastruct.TypeVector {
		return nil, false, errVectorType()
	}
	m, derr := datastruct.DecodeVector(e.Payload)
	if derr != nil {
		return nil, false, errVectorType()
	}
	return m, true, protocol.Value{}
}

func (h *vectorHandler) saveVector(ctx context.Context, key string, m map[string]datastruct.VEntry) protocol.Value {
	payload, err := datastruct.EncodeVector(m)
	if err != nil {
		return errValueStr("ERR " + err.Error())
	}
	if err := h.kv.Set(ctx, datastruct.VectorKey(key),
		datastruct.Encode(datastruct.TypeVector, 0, payload)); err != nil {
		return errValue(err)
	}
	return protocol.Value{}
}

// parseFP32Bulk 小端 float32 数组转 float64（FP32 输入形态）。
func parseFP32Bulk(b []byte) ([]float64, bool) {
	if len(b)%4 != 0 || len(b) == 0 {
		return nil, false
	}
	out := make([]float64, 0, len(b)/4)
	for i := 0; i < len(b); i += 4 {
		out = append(out, float64(math.Float32frombits(binary.LittleEndian.Uint32(b[i:]))))
	}
	return out, true
}

// parseValuesBulk 空格分隔浮点文本转向量（VALUES num 形态）。
func parseValuesBulk(s string, num int) ([]float64, bool) {
	fs := strings.Fields(s)
	if num >= 0 && len(fs) != num {
		return nil, false
	}
	out := make([]float64, 0, len(fs))
	for _, f := range fs {
		v, err := strconv.ParseFloat(f, 64)
		if err != nil {
			return nil, false
		}
		out = append(out, v)
	}
	return out, true
}

// vadd `VADD key [FP32|VALUES num] vector element`：只收两种输入形态，
// 其余选项（REDUCE/CAS/NOQUANT/Q8/BIN/EF/SETATTR/M）一律 syntax error。
// 成功回 1（新增）/ 0（已存在覆盖）。
func (h *vectorHandler) vadd(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'vadd' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	rest := args[1:]
	var vec []float64
	var elem string
	var parsed bool
	if kw, _ := argString(rest[0]); len(rest) >= 3 && strings.EqualFold(kw, "FP32") {
		b, ok := argBulk(rest[1])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		vec, parsed = parseFP32Bulk(b)
		elem, ok = argString(rest[2])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		rest = rest[3:]
	} else if kw, _ := argString(rest[0]); len(rest) >= 4 && strings.EqualFold(kw, "VALUES") {
		nStr, _ := argString(rest[1])
		n, err := strconv.Atoi(nStr)
		if err != nil || n < 0 {
			return errValueStr("ERR syntax error")
		}
		s, ok := argString(rest[2])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		vec, parsed = parseValuesBulk(s, n)
		elem, ok = argString(rest[3])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		rest = rest[4:]
	}
	if !parsed || len(rest) != 0 || elem == "" {
		return errValueStr("ERR syntax error")
	}
	m, found, errVal := h.loadVector(ctx, key)
	if errVal.Kind != 0 {
		return errVal
	}
	if !found {
		m = map[string]datastruct.VEntry{}
	} else if len(m) > 0 {
		for _, e := range m {
			if len(e.Vec) != len(vec) {
				return errValueStr("ERR vector dimension mismatch")
			}
			break
		}
	}
	_, exists := m[elem]
	m[elem] = datastruct.VEntry{Vec: vec}
	if errVal := h.saveVector(ctx, key, m); errVal.Kind != 0 {
		return errVal
	}
	if exists {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 1}
}

// vcard/vdim 缺键回 0（真机口径）。
func (h *vectorHandler) vcard(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'vcard' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, found, errVal := h.loadVector(ctx, key)
	if errVal.Kind != 0 {
		return errVal
	}
	if !found {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(m))}
}

func (h *vectorHandler) vdim(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'vdim' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, found, errVal := h.loadVector(ctx, key)
	if errVal.Kind != 0 {
		return errVal
	}
	if !found || len(m) == 0 {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	for _, e := range m {
		return protocol.Value{Kind: protocol.KindInteger, I: int64(len(e.Vec))}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 0}
}

// vemb 缺键/缺元素回 nil bulk（VEMB 单值语义）。
func (h *vectorHandler) vemb(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'vemb' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	elem, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	m, found, errVal := h.loadVector(ctx, key)
	if errVal.Kind != 0 {
		return errVal
	}
	if !found {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	e, ok := m[elem]
	if !ok {
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	out := make([]string, 0, len(e.Vec))
	for _, v := range e.Vec {
		out = append(out, strconv.FormatFloat(v, 'g', -1, 64))
	}
	return toBulkArray(out)
}

func (h *vectorHandler) vrem(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'vrem' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	elem, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	m, found, errVal := h.loadVector(ctx, key)
	if errVal.Kind != 0 {
		return errVal
	}
	if !found {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	if _, ok := m[elem]; !ok {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	delete(m, elem)
	if errVal := h.saveVector(ctx, key, m); errVal.Kind != 0 {
		return errVal
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 1}
}

// vrandmember 仿 SRANDMEMBER 缺键语义：无 count 缺键回 nil bulk；
// 有 count 缺键回空数组；count 越界 clamp；负数可重复。
func (h *vectorHandler) vrandmember(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return errValueStr("ERR wrong number of arguments for 'vrandmember' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	count := 0
	hasCount := false
	if len(args) == 2 {
		nStr, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR value is not an integer or out of range")
		}
		n, err := strconv.Atoi(nStr)
		if err != nil {
			return errValueStr("ERR value is not an integer or out of range")
		}
		count, hasCount = n, true
	}
	m, found, errVal := h.loadVector(ctx, key)
	if errVal.Kind != 0 {
		return errVal
	}
	if !found {
		if hasCount {
			return toBulkArray(nil)
		}
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	if !hasCount {
		if len(names) == 0 {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return protocol.BulkOf(names[rand.Intn(len(names))])
	}
	if count >= 0 {
		if count > len(names) {
			count = len(names)
		}
		perm := rand.Perm(len(names))
		out := make([]string, 0, count)
		for _, i := range perm[:count] {
			out = append(out, names[i])
		}
		return toBulkArray(out)
	}
	out := make([]string, 0, -count)
	for i := 0; i < -count; i++ {
		out = append(out, names[rand.Intn(len(names))])
	}
	return toBulkArray(out)
}

// cosSim 余弦相似度；零向量回 0（避免 NaN 进排序）。
func cosSim(a, b []float64) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// vsim `VSIM key (ELE element|FP32 vec|VALUES num vec) [WITHSCORES] [COUNT n]`：
// 暴力精确扫描（P0 恒等 TRUTH，EF/TRUTH/NOTHREAD 等收但忽略——
// 忽略≠支持：FILTER/FILTER-EF/EPSILON 等一律 syntax error，文档声明）。
func (h *vectorHandler) vsim(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'vsim' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	m, found, errVal := h.loadVector(ctx, key)
	if errVal.Kind != 0 {
		return errVal
	}
	if !found {
		return toBulkArray(nil)
	}
	rest := args[1:]
	var target []float64
	var parsed bool
	if kw, _ := argString(rest[0]); strings.EqualFold(kw, "ELE") {
		if len(rest) < 2 {
			return errValueStr("ERR syntax error")
		}
		elem, ok := argString(rest[1])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		e, ok := m[elem]
		if !ok {
			return errValueStr(fmt.Sprintf("ERR No such element '%s'", elem))
		}
		target, parsed = e.Vec, true
		rest = rest[2:]
	} else if kw, _ := argString(rest[0]); strings.EqualFold(kw, "FP32") {
		if len(rest) < 2 {
			return errValueStr("ERR syntax error")
		}
		b, ok := argBulk(rest[1])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		target, parsed = parseFP32Bulk(b)
		rest = rest[2:]
	} else if kw, _ := argString(rest[0]); strings.EqualFold(kw, "VALUES") {
		if len(rest) < 3 {
			return errValueStr("ERR syntax error")
		}
		nStr, _ := argString(rest[1])
		n, err := strconv.Atoi(nStr)
		if err != nil || n < 0 {
			return errValueStr("ERR syntax error")
		}
		s, ok := argString(rest[2])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		target, parsed = parseValuesBulk(s, n)
		rest = rest[3:]
	}
	if !parsed {
		return errValueStr("ERR syntax error")
	}
	withScores := false
	count := -1
	for len(rest) > 0 {
		kw, ok := argString(rest[0])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch {
		case strings.EqualFold(kw, "WITHSCORES"):
			withScores = true
			rest = rest[1:]
		case strings.EqualFold(kw, "COUNT"):
			if len(rest) < 2 {
				return errValueStr("ERR syntax error")
			}
			nStr, ok := argString(rest[1])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			n, err := strconv.Atoi(nStr)
			if err != nil || n < 0 {
				return errValueStr("ERR syntax error")
			}
			count = n
			rest = rest[2:]
		case strings.EqualFold(kw, "TRUTH") || strings.EqualFold(kw, "NOTHREAD"):
			// P0 本就是精确扫描：收下忽略（文档声明）。
			rest = rest[1:]
		default:
			return errValueStr("ERR syntax error")
		}
	}
	type scored struct {
		name  string
		score float64
	}
	all := make([]scored, 0, len(m))
	for name, e := range m {
		if len(e.Vec) != len(target) {
			continue
		}
		all = append(all, scored{name, cosSim(target, e.Vec)})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].score == all[j].score {
			return all[i].name < all[j].name
		}
		return all[i].score > all[j].score
	})
	if count >= 0 && count < len(all) {
		all = all[:count]
	}
	out := make([]string, 0, len(all))
	for _, s := range all {
		out = append(out, s.name)
		if withScores {
			out = append(out, formatScore(s.score))
		}
	}
	return toBulkArray(out)
}

// argBulk 取 bulk 原字节（FP32 二进制输入用）。
func argBulk(v protocol.Value) ([]byte, bool) {
	if v.Kind != protocol.KindBulkString {
		return nil, false
	}
	return v.Bulk, true
}
