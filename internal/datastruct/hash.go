package datastruct

import (
	"encoding/binary"
	"fmt"
)

const (
	EncodingListpack  byte = 0
	EncodingHashtable byte = 1
)

const (
	hashMaxListpackEntries = 128
	hashMaxListpackValue   = 64
)

// EncodeHash 把 field-value 映射编码为 [encoding][uvarint count][field/value 长度前缀对]。
// 自适应规则照抄 Redis 默认：>128 个 field 或任一 field/value >64B 用 hashtable。
func EncodeHash(m map[string]string) []byte {
	enc := EncodingListpack
	if len(m) > hashMaxListpackEntries {
		enc = EncodingHashtable
	} else {
		for f, v := range m {
			if len(f) > hashMaxListpackValue || len(v) > hashMaxListpackValue {
				enc = EncodingHashtable
				break
			}
		}
	}
	size := 1 + binary.MaxVarintLen64
	for f, v := range m {
		size += binary.MaxVarintLen64 + len(f) + binary.MaxVarintLen64 + len(v)
	}
	out := make([]byte, 0, size)
	out = append(out, enc)
	out = binary.AppendUvarint(out, uint64(len(m)))
	fields := make([]string, 0, len(m))
	for f := range m {
		fields = append(fields, f)
	}
	sortStrings(fields)
	for _, f := range fields {
		v := m[f]
		out = binary.AppendUvarint(out, uint64(len(f)))
		out = append(out, f...)
		out = binary.AppendUvarint(out, uint64(len(v)))
		out = append(out, v...)
	}
	return out
}

// DecodeHash 解析 EncodeHash 的输出，返回映射与编码标签。
func DecodeHash(raw []byte) (map[string]string, byte, error) {
	if len(raw) < 1 {
		return nil, 0, fmt.Errorf("datastruct: truncated hash payload")
	}
	enc := raw[0]
	rest := raw[1:]
	n, adv := binary.Uvarint(rest)
	if adv <= 0 {
		return nil, 0, fmt.Errorf("datastruct: bad hash count")
	}
	rest = rest[adv:]
	m := make(map[string]string, n)
	for i := uint64(0); i < n; i++ {
		f, rest2, ok := readHashStr(rest)
		if !ok {
			return nil, 0, fmt.Errorf("datastruct: truncated hash field %d", i)
		}
		v, rest3, ok := readHashStr(rest2)
		if !ok {
			return nil, 0, fmt.Errorf("datastruct: truncated hash value %d", i)
		}
		m[f] = v
		rest = rest3
	}
	return m, enc, nil
}

func readHashStr(b []byte) (string, []byte, bool) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 || uint64(len(b[adv:])) < n {
		return "", nil, false
	}
	return string(b[adv : adv+int(n)]), b[adv+int(n):], true
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// HashKey 给用户 key 加 h: 前缀。
func HashKey(key string) []byte {
	return append([]byte("h:"), key...)
}

// HashExpKey 给用户 key 加 x: 前缀（hash field 过期 sidecar）。
// x: 与任何类型前缀都不重叠，Scan/KEYS 扫不到。
func HashExpKey(key string) []byte {
	return append([]byte("x:"), key...)
}
