package datastruct

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// VEntry 是 vectorset 的一个元素：全精度向量 + 可选 JSON 属性原文（"" 表无）。
type VEntry struct {
	Vec  []float64
	Attr string
}

// VectorKey 给用户 key 加 vs: 前缀。
func VectorKey(key string) []byte { return append([]byte("vs:"), key...) }

// EncodeVector 把元素表编码为 [uvarint count][条目...]，条目 =
// [uvarint dim][dim × float64LE][uvarint namelen][name][uvarint attrlen][attr]。
// 元素按名排序保证输出确定性；空表合法；各向量维度必须一致。
func EncodeVector(m map[string]VEntry) ([]byte, error) {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	dim := -1
	for _, n := range names {
		d := len(m[n].Vec)
		if dim < 0 {
			dim = d
		} else if d != dim {
			return nil, fmt.Errorf("datastruct: vectorset dim mismatch %d != %d", d, dim)
		}
	}
	out := binary.AppendUvarint(nil, uint64(len(names)))
	for _, n := range names {
		e := m[n]
		out = binary.AppendUvarint(out, uint64(len(e.Vec)))
		for _, v := range e.Vec {
			out = binary.AppendUvarint(out, math.Float64bits(v))
		}
		out = binary.AppendUvarint(out, uint64(len(n)))
		out = append(out, n...)
		out = binary.AppendUvarint(out, uint64(len(e.Attr)))
		out = append(out, e.Attr...)
	}
	return out, nil
}

// DecodeVector 解析 EncodeVector 的输出，返回元素表；维度不一致或截断报错。
func DecodeVector(raw []byte) (map[string]VEntry, error) {
	out := map[string]VEntry{}
	rest := raw
	count, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, fmt.Errorf("datastruct: truncated vectorset count")
	}
	rest = rest[n:]
	dim := -1
	for i := uint64(0); i < count; i++ {
		d, n := binary.Uvarint(rest)
		if n <= 0 {
			return nil, fmt.Errorf("datastruct: truncated vectorset dim")
		}
		rest = rest[n:]
		if dim < 0 {
			dim = int(d)
		} else if int(d) != dim {
			return nil, fmt.Errorf("datastruct: vectorset dim mismatch %d != %d", d, dim)
		}
		vec := make([]float64, 0, d)
		for j := uint64(0); j < d; j++ {
			bits, n := binary.Uvarint(rest)
			if n <= 0 {
				return nil, fmt.Errorf("datastruct: truncated vectorset value")
			}
			rest = rest[n:]
			vec = append(vec, math.Float64frombits(bits))
		}
		nl, n := binary.Uvarint(rest)
		if n <= 0 || uint64(len(rest[n:])) < nl {
			return nil, fmt.Errorf("datastruct: truncated vectorset name")
		}
		name := string(rest[n : n+int(nl)])
		rest = rest[n+int(nl):]
		al, n := binary.Uvarint(rest)
		if n <= 0 || uint64(len(rest[n:])) < al {
			return nil, fmt.Errorf("datastruct: truncated vectorset attr")
		}
		attr := string(rest[n : n+int(al)])
		rest = rest[n+int(al):]
		out[name] = VEntry{Vec: vec, Attr: attr}
	}
	return out, nil
}
