package datastruct

import (
	"encoding/binary"
	"fmt"
	"math"
)

// HLL dense-only 实现，逐行对标 Redis 7.2 hyperloglog.c（dense 部分）：
// MurmurHash64A（小端、seed 0xadc83b19）、p=14/16384 寄存器、6-bit 打包、
// Ertl sigma/tau 估计器。Sparse 留待后续，该文件 API 保持表示无关。

const (
	hllP         = 14
	hllRegisters = 1 << hllP
	hllPisteMask = hllRegisters - 1
	hllQ         = 64 - hllP
	hllBits      = 6
	hllRegMax    = (1 << hllBits) - 1

	hllHdrSize   = 16
	hllDenseRegs = (hllRegisters*hllBits + 7) / 8

	hllDenseEncoding = 0

	hllAlphaInf = 0.721347520444481703680
)

// HLLNewDense 返回 12289 字节的零寄存器（含 1  spare 字节，供末寄存器 b1 安全读取）。
func HLLNewDense() []byte {
	return make([]byte, hllDenseRegs+1)
}

func murmurHash64A(key []byte, seed uint32) uint64 {
	const m = 0xc6a4a7935bd1e995
	const r = 47
	h := uint64(seed) ^ (uint64(len(key)) * m)
	n := len(key) &^ 7
	for i := 0; i < n; i += 8 {
		k := binary.LittleEndian.Uint64(key[i:])
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
	}
	tail := key[n:]
	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

// hllPatLen 返回元素哈希的 000..1 游程与寄存器下标。
func hllPatLen(ele []byte) (index int, count uint8) {
	hash := murmurHash64A(ele, 0xadc83b19)
	index = int(hash & hllPisteMask)
	hash >>= hllP
	hash |= uint64(1) << hllQ
	bit := uint64(1)
	count = 1
	for hash&bit == 0 {
		count++
		bit <<= 1
	}
	return index, count
}

func hllDenseGet(reg []byte, pos int) uint8 {
	b0 := int(pos * hllBits / 8)
	fb := uint(pos*hllBits) & 7
	fb8 := 8 - fb
	var b1 byte
	if b0+1 < len(reg) {
		b1 = reg[b0+1]
	}
	return uint8((uint(reg[b0])>>fb)|(uint(b1)<<fb8)) & hllRegMax
}

func hllDenseSet(reg []byte, pos int, val uint8) {
	b0 := pos * hllBits / 8
	fb := uint(pos*hllBits) & 7
	fb8 := 8 - fb
	v := uint(val)
	reg[b0] &= ^uint8(hllRegMax << fb)
	reg[b0] |= uint8(v << fb)
	if b0+1 < len(reg) {
		reg[b0+1] &= ^uint8(hllRegMax >> fb8)
		reg[b0+1] |= uint8(v >> fb8)
	}
}

// HLLDenseAdd 加入元素，寄存器变化返回 true。
func HLLDenseAdd(reg []byte, ele []byte) bool {
	index, count := hllPatLen(ele)
	if count > hllDenseGet(reg, index) {
		hllDenseSet(reg, index, count)
		return true
	}
	return false
}

// HLLDenseMerge 把 src 按寄存器取 max 并入 dst，变化返回 true。
func HLLDenseMerge(dst, src []byte) bool {
	changed := false
	for i := 0; i < hllRegisters; i++ {
		s := hllDenseGet(src, i)
		if s > hllDenseGet(dst, i) {
			hllDenseSet(dst, i, s)
			changed = true
		}
	}
	return changed
}

func hllSigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y := 1.0
	z := x
	var zPrime float64
	for {
		x *= x
		zPrime = z
		z += x * y
		y += y
		if zPrime == z {
			break
		}
	}
	return z
}

func hllTau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y := 1.0
	z := 1 - x
	var zPrime float64
	for {
		x = math.Sqrt(x)
		zPrime = z
		y *= 0.5
		z -= math.Pow(1-x, 2) * y
		if zPrime == z {
			break
		}
	}
	return z / 3
}

// HLLDenseCount 用 Ertl 估计器计算基数（官方容差内与 Redis 一致）。
func HLLDenseCount(reg []byte) uint64 {
	m := float64(hllRegisters)
	var histo [64]int
	for j := 0; j < hllRegisters; j++ {
		histo[hllDenseGet(reg, j)]++
	}
	z := m * hllTau((m-float64(histo[hllQ+1]))/m)
	for j := hllQ; j >= 1; j-- {
		z += float64(histo[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(histo[0])/m)
	return uint64(math.Round(hllAlphaInf * m * m / z))
}

// EncodeHLL 组装 [HYLL][encoding=0][3保留][8LE card=0] + 12288 寄存器。
func EncodeHLL(reg []byte) []byte {
	out := make([]byte, 0, hllHdrSize+hllDenseRegs)
	out = append(out, 'H', 'Y', 'L', 'L', hllDenseEncoding, 0, 0, 0)
	var card [8]byte
	out = append(out, card[:]...)
	for i := 0; i < hllDenseRegs; i++ {
		var b byte
		if i < len(reg) {
			b = reg[i]
		}
		out = append(out, b)
	}
	return out
}

// DecodeHLL 校验魔数与 dense 编码，返回 12289 字节寄存器（含 spare）。
func DecodeHLL(raw []byte) ([]byte, error) {
	if len(raw) != hllHdrSize+hllDenseRegs {
		return nil, fmt.Errorf("WRONGTYPE Key is not a valid HyperLogLog string value.")
	}
	if raw[0] != 'H' || raw[1] != 'Y' || raw[2] != 'L' || raw[3] != 'L' {
		return nil, fmt.Errorf("WRONGTYPE Key is not a valid HyperLogLog string value.")
	}
	if raw[4] != hllDenseEncoding {
		return nil, fmt.Errorf("WRONGTYPE Key is not a valid HyperLogLog string value.")
	}
	reg := make([]byte, hllDenseRegs+1)
	copy(reg, raw[hllHdrSize:])
	return reg, nil
}

// HLLKey 给用户 key 加 hll: 前缀。
func HLLKey(key string) []byte {
	return append([]byte("hll:"), key...)
}
