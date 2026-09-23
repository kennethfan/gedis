package datastruct

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

const (
	ZSetEncodingListpack byte = 0
	ZSetEncodingSkiplist byte = 1
)

const (
	zsetMaxListpackEntries = 128
	zsetMaxListpackValue   = 64
)

// EncodeZSet 把 member->score 映射编码为 [encoding][uvarint count][member/score 对序列]。
// score 存 8 字节大端 float64 位；输出按 (score, member) 排序保证确定性。
// 自适应规则照抄 Hash：>128 个 member 或任一 member >64B 用 skiplist。
func EncodeZSet(z map[string]float64) []byte {
	enc := ZSetEncodingListpack
	if len(z) > zsetMaxListpackEntries {
		enc = ZSetEncodingSkiplist
	} else {
		for m := range z {
			if len(m) > zsetMaxListpackValue {
				enc = ZSetEncodingSkiplist
				break
			}
		}
	}
	pairs := make([]zsetPair, 0, len(z))
	for m, s := range z {
		pairs = append(pairs, zsetPair{m: m, s: s})
	}
	sortZSetPairs(pairs)

	size := 1 + binary.MaxVarintLen64
	for _, p := range pairs {
		size += binary.MaxVarintLen64 + len(p.m) + 8
	}
	out := make([]byte, 0, size)
	out = append(out, enc)
	out = binary.AppendUvarint(out, uint64(len(pairs)))
	for _, p := range pairs {
		out = binary.AppendUvarint(out, uint64(len(p.m)))
		out = append(out, p.m...)
		out = binary.BigEndian.AppendUint64(out, math.Float64bits(p.s))
	}
	return out
}

// DecodeZSet 解析 EncodeZSet 的输出，返回映射与编码标签。
func DecodeZSet(raw []byte) (map[string]float64, byte, error) {
	if len(raw) < 1 {
		return nil, 0, fmt.Errorf("datastruct: truncated zset payload")
	}
	enc := raw[0]
	rest := raw[1:]
	n, adv := binary.Uvarint(rest)
	if adv <= 0 {
		return nil, 0, fmt.Errorf("datastruct: bad zset count")
	}
	rest = rest[adv:]
	z := make(map[string]float64, n)
	for i := uint64(0); i < n; i++ {
		mlen, adv := binary.Uvarint(rest)
		if adv <= 0 || uint64(len(rest[adv:])) < mlen+8 {
			return nil, 0, fmt.Errorf("datastruct: truncated zset member %d", i)
		}
		m := string(rest[adv : adv+int(mlen)])
		rest = rest[adv+int(mlen):]
		s := math.Float64frombits(binary.BigEndian.Uint64(rest[:8]))
		rest = rest[8:]
		z[m] = s
	}
	return z, enc, nil
}

type zsetPair struct {
	m string
	s float64
}

func sortZSetPairs(p []zsetPair) {
	for i := 1; i < len(p); i++ {
		for j := i; j > 0 && (p[j].s < p[j-1].s || (p[j].s == p[j-1].s && p[j].m < p[j-1].m)); j-- {
			p[j], p[j-1] = p[j-1], p[j]
		}
	}
}

// ParseScore 解析 ZADD 写入的 score：NaN/±Inf 按 #15 约定拒绝（命令层统一入口）。
func ParseScore(s string) (float64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not a valid float")
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("ERR value is not a valid float")
	}
	return f, nil
}

// ZSetKey 给用户 key 加 z: 前缀。
func ZSetKey(key string) []byte {
	return append([]byte("z:"), key...)
}
