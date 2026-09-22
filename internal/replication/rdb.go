package replication

import (
	"encoding/binary"
	"fmt"
)

// RawEntry 是一条全量同步的 KV：Key 为带类型前缀的存储 key，
// Value 为完整 entry 字节（含类型标签与过期时间）。
type RawEntry struct {
	Key   []byte
	Value []byte
}

// MarshalRDB 编码全量数据：[uvarint 条数][uvarint klen][key][uvarint vlen][value]…。
func MarshalRDB(entries []RawEntry) []byte {
	size := binary.MaxVarintLen64
	for _, e := range entries {
		size += binary.MaxVarintLen64 + len(e.Key) + binary.MaxVarintLen64 + len(e.Value)
	}
	out := make([]byte, 0, size)
	out = binary.AppendUvarint(out, uint64(len(entries)))
	for _, e := range entries {
		out = binary.AppendUvarint(out, uint64(len(e.Key)))
		out = append(out, e.Key...)
		out = binary.AppendUvarint(out, uint64(len(e.Value)))
		out = append(out, e.Value...)
	}
	return out
}

// UnmarshalRDB 解析 MarshalRDB 的输出。
func UnmarshalRDB(raw []byte) ([]RawEntry, error) {
	n, adv := binary.Uvarint(raw)
	if adv <= 0 {
		return nil, fmt.Errorf("replication: bad rdb count")
	}
	rest := raw[adv:]
	out := make([]RawEntry, 0, n)
	for i := uint64(0); i < n; i++ {
		k, rest2, ok := rdbBytes(rest)
		if !ok {
			return nil, fmt.Errorf("replication: truncated rdb key %d", i)
		}
		v, rest3, ok := rdbBytes(rest2)
		if !ok {
			return nil, fmt.Errorf("replication: truncated rdb value %d", i)
		}
		out = append(out, RawEntry{Key: k, Value: v})
		rest = rest3
	}
	return out, nil
}

func rdbBytes(b []byte) ([]byte, []byte, bool) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 || uint64(len(b[adv:])) < n {
		return nil, nil, false
	}
	cp := append([]byte(nil), b[adv:adv+int(n)]...)
	return cp, b[adv+int(n):], true
}
