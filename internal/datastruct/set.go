package datastruct

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

const (
	EncodingIntset byte = 0
)

const (
	setMaxIntsetEntries = 512
)

// EncodeSet 把成员列表编码为 [encoding][uvarint count][成员长度前缀序列]。
// 自适应规则：全员可解析为 int64 且不超过 512 个用 intset，否则用 hashtable。
// 成员去重后按字典序排列，保证输出确定性。
func EncodeSet(members []string) []byte {
	uniq := make(map[string]struct{}, len(members))
	for _, m := range members {
		uniq[m] = struct{}{}
	}
	sorted := make([]string, 0, len(uniq))
	for m := range uniq {
		sorted = append(sorted, m)
	}
	sortStrings(sorted)

	enc := EncodingHashtable
	if len(sorted) <= setMaxIntsetEntries && allInt64(sorted) {
		enc = EncodingIntset
	}

	size := 1 + binary.MaxVarintLen64
	for _, m := range sorted {
		size += binary.MaxVarintLen64 + len(m)
	}
	out := make([]byte, 0, size)
	out = append(out, enc)
	out = binary.AppendUvarint(out, uint64(len(sorted)))
	for _, m := range sorted {
		out = binary.AppendUvarint(out, uint64(len(m)))
		out = append(out, m...)
	}
	return out
}

// DecodeSet 解析 EncodeSet 的输出，返回成员集合与编码标签。
func DecodeSet(raw []byte) (map[string]struct{}, byte, error) {
	if len(raw) < 1 {
		return nil, 0, fmt.Errorf("datastruct: truncated set payload")
	}
	enc := raw[0]
	rest := raw[1:]
	n, adv := binary.Uvarint(rest)
	if adv <= 0 {
		return nil, 0, fmt.Errorf("datastruct: bad set count")
	}
	rest = rest[adv:]
	set := make(map[string]struct{}, n)
	for i := uint64(0); i < n; i++ {
		m, rest2, ok := readSetStr(rest)
		if !ok {
			return nil, 0, fmt.Errorf("datastruct: truncated set member %d", i)
		}
		set[m] = struct{}{}
		rest = rest2
	}
	return set, enc, nil
}

func readSetStr(b []byte) (string, []byte, bool) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 || uint64(len(b[adv:])) < n {
		return "", nil, false
	}
	return string(b[adv : adv+int(n)]), b[adv+int(n):], true
}

func allInt64(members []string) bool {
	for _, m := range members {
		if _, err := strconv.ParseInt(m, 10, 64); err != nil {
			return false
		}
	}
	return true
}

// SetKey 给用户 key 加 st: 前缀。
func SetKey(key string) []byte {
	return append([]byte("st:"), key...)
}
