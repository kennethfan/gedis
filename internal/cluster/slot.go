package cluster

import (
	"crypto/sha1"
	"fmt"
	"strconv"
	"strings"
)

// key→slot 映射，逐行对标 Redis 7.2 cluster.c（keyHashSlot / extractKeyTag）：
// hash-tag `{…}` 抽取 + CRC16-XMODEM（poly 0x1021，初值 0），结果 & 16383。
// 叶子包：零内部依赖，供 network 层重定向 hook、命令侧 key 抽取、配置校验共用。

// NumSlots 是 Cluster 总槽位数。
const NumSlots = 16384

// Slot 返回 key 所属的槽位（0 <= n < NumSlots）。
func Slot(key string) int {
	return int(crc16(tag(key)) & (NumSlots - 1))
}

// tag 按 Redis 规则抽取 hash-tag：首个 '{' 之后的首个 '}' 之间非空即为 tag；
// 否则（无括号、空括号、括号不配对）用整个 key 计算。
func tag(key string) string {
	if i := indexByte(key, '{'); i >= 0 {
		if j := indexByte(key[i+1:], '}'); j > 0 {
			return key[i+1 : i+1+j]
		}
	}
	return key
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// ParseSlotRanges 解析混合段记法（#40）："0-5460" 闭区间、"7001" 单槽。
// 越界/倒置/非数字直接报错（配置加载期 fail-fast）。
func ParseSlotRanges(specs []string) ([][2]int, error) {
	var out [][2]int
	for _, s := range specs {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("cluster: empty slot spec")
		}
		if i := strings.IndexByte(s, '-'); i >= 0 {
			lo, err1 := strconv.Atoi(strings.TrimSpace(s[:i]))
			hi, err2 := strconv.Atoi(strings.TrimSpace(s[i+1:]))
			if err1 != nil || err2 != nil {
				return nil, fmt.Errorf("cluster: invalid slot range %q", s)
			}
			if lo < 0 || hi >= NumSlots || lo > hi {
				return nil, fmt.Errorf("cluster: slot range %q out of [0,%d]", s, NumSlots-1)
			}
			out = append(out, [2]int{lo, hi})
			continue
		}
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 || n >= NumSlots {
			return nil, fmt.Errorf("cluster: invalid slot %q", s)
		}
		out = append(out, [2]int{n, n})
	}
	return out, nil
}

// DeriveID 由 addr 稳定派生 40 位 hex 节点 ID（sha1，与真机 ID 同形）。
// 配置省略 id 时用它，保证重启不变。
func DeriveID(addr string) string {
	sum := sha1.Sum([]byte(addr))
	const hexd = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(40)
	for _, b := range sum {
		sb.WriteByte(hexd[b>>4])
		sb.WriteByte(hexd[b&0x0f])
	}
	return sb.String()
}

// crc16 是 CRC16-XMODEM（poly 0x1021，初值 0x0000），对标 crc16.c。
func crc16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for k := 0; k < 8; k++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
