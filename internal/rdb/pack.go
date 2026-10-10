package rdb

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

// ---------- listpack ----------

// parseListpack 解析一段完整的 listpack blob，返回条目字符串（整数转十进制）。
func parseListpack(blob []byte) ([]string, error) {
	if len(blob) < 7 {
		return nil, fmt.Errorf("rdb: truncated listpack")
	}
	total := binary.LittleEndian.Uint32(blob[0:4])
	if uint64(total) != uint64(len(blob)) {
		return nil, fmt.Errorf("rdb: listpack size mismatch %d != %d", total, len(blob))
	}
	if blob[len(blob)-1] != 0xff {
		return nil, fmt.Errorf("rdb: listpack missing EOF")
	}
	var out []string
	pos := 6
	end := len(blob) - 1
	for pos < end {
		entryStart := pos
		s, next, err := listpackEntry(blob, pos, end)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
		// 跳 backlen：单字节仅当值 ≤127（高位无标记），多字节大端 7bit 组、
		// 末字节高位置位。正向无法靠标记区分 1 字节与多字节首字节，
		// 故按条目长度定宽跳过并回验值（防错位静默）。
		entryLen := next - entryStart
		sz := 1
		for v := entryLen; v > 127; v >>= 7 {
			sz++
		}
		if next+sz > len(blob) {
			return nil, fmt.Errorf("rdb: listpack backlen overrun")
		}
		dec := 0
		for k := 0; k < sz; k++ {
			b := blob[next+k]
			dec = (dec << 7) | int(b&0x7f)
			if k == sz-1 {
				if sz > 1 && b&0x80 == 0 {
					return nil, fmt.Errorf("rdb: listpack backlen missing terminator")
				}
			} else if b&0x80 != 0 {
				return nil, fmt.Errorf("rdb: listpack backlen early terminator")
			}
		}
		if dec != entryLen {
			return nil, fmt.Errorf("rdb: listpack backlen mismatch %d != %d", dec, entryLen)
		}
		pos = next + sz
	}
	return out, nil
}

// listpackEntry 解析单个条目，返回 (字符串, 条目后位置)。
func listpackEntry(blob []byte, pos, end int) (string, int, error) {
	if pos >= end {
		return "", 0, fmt.Errorf("rdb: listpack entry overrun")
	}
	b := blob[pos]
	pos++
	switch {
	case b&0x80 == 0:
		return strconv.FormatUint(uint64(b&0x7f), 10), pos, nil
	case b&0xc0 == 0x80:
		n := int(b & 0x3f)
		if pos+n > end {
			return "", 0, fmt.Errorf("rdb: listpack str overrun")
		}
		s := string(blob[pos : pos+n])
		return s, pos + n, nil
	case b&0xe0 == 0xc0:
		if pos >= end {
			return "", 0, fmt.Errorf("rdb: listpack 13bit overrun")
		}
		v := int(int(b&0x1f)<<8 | int(blob[pos]))
		pos++
		// 13bit 有符号：raw≥4096 表负数（raw-8192），否则为正。
		// stream 主条目 ms/seq 差值为负即走此分支，既有 P0 数据全为正故未暴露。
		if v >= 4096 {
			v -= 8192
		}
		return strconv.FormatInt(int64(v), 10), pos, nil
	case b&0xf0 == 0xe0:
		if pos >= end {
			return "", 0, fmt.Errorf("rdb: listpack 12bit overrun")
		}
		n := int(b&0x0f)<<8 | int(blob[pos])
		pos++
		if pos+n > end {
			return "", 0, fmt.Errorf("rdb: listpack str12 overrun")
		}
		s := string(blob[pos : pos+n])
		return s, pos + n, nil
	}
	switch b {
	case 0xf0:
		if pos+2 > end {
			return "", 0, fmt.Errorf("rdb: listpack i16 overrun")
		}
		v := int16(binary.LittleEndian.Uint16(blob[pos:]))
		return strconv.FormatInt(int64(v), 10), pos + 2, nil
	case 0xf1:
		if pos+3 > end {
			return "", 0, fmt.Errorf("rdb: listpack i24 overrun")
		}
		u := uint32(blob[pos]) | uint32(blob[pos+1])<<8 | uint32(blob[pos+2])<<16
		var v int32
		if u&0x800000 != 0 {
			v = int32(u | 0xff000000)
		} else {
			v = int32(u)
		}
		return strconv.FormatInt(int64(v), 10), pos + 3, nil
	case 0xf2:
		if pos+4 > end {
			return "", 0, fmt.Errorf("rdb: listpack i32 overrun")
		}
		v := int32(binary.LittleEndian.Uint32(blob[pos:]))
		return strconv.FormatInt(int64(v), 10), pos + 4, nil
	case 0xf3:
		if pos+8 > end {
			return "", 0, fmt.Errorf("rdb: listpack i64 overrun")
		}
		v := int64(binary.LittleEndian.Uint64(blob[pos:]))
		return strconv.FormatInt(v, 10), pos + 8, nil
	case 0xf4:
		if pos+4 > end {
			return "", 0, fmt.Errorf("rdb: listpack str32 overrun")
		}
		n := int(binary.LittleEndian.Uint32(blob[pos:]))
		pos += 4
		if pos+n > end {
			return "", 0, fmt.Errorf("rdb: listpack str32 body overrun")
		}
		s := string(blob[pos : pos+n])
		return s, pos + n, nil
	default:
		return "", 0, fmt.Errorf("rdb: unknown listpack encoding %#02x", b)
	}
}

// parseListpackDouble 解析 zset listpack 中的 score 项（真机存十进制串）。
func parseListpackDouble(s string) (float64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("rdb: bad zset score %q", s)
	}
	return f, nil
}

// ---------- intset ----------

// parseIntset 解析 intset blob（encoding u32LE + length u32LE + 元素 LE）。
func parseIntset(blob []byte) ([]string, error) {
	if len(blob) < 8 {
		return nil, fmt.Errorf("rdb: truncated intset")
	}
	enc := binary.LittleEndian.Uint32(blob[0:4])
	n := binary.LittleEndian.Uint32(blob[4:8])
	var width int
	switch enc {
	case 2:
		width = 2
	case 4:
		width = 4
	case 8:
		width = 8
	default:
		return nil, fmt.Errorf("rdb: bad intset encoding %d", enc)
	}
	if uint64(8)+uint64(n)*uint64(width) != uint64(len(blob)) {
		return nil, fmt.Errorf("rdb: intset size mismatch")
	}
	out := make([]string, 0, n)
	pos := 8
	for i := uint32(0); i < n; i++ {
		var v int64
		switch width {
		case 2:
			v = int64(int16(binary.LittleEndian.Uint16(blob[pos:])))
		case 4:
			v = int64(int32(binary.LittleEndian.Uint32(blob[pos:])))
		case 8:
			v = int64(binary.LittleEndian.Uint64(blob[pos:]))
		}
		out = append(out, strconv.FormatInt(v, 10))
		pos += width
	}
	return out, nil
}

// ---------- quicklist ----------

// readQuicklist 读 quicklist2：节点数 + 每节点 (container, 数据)。
// container 1=plain 字符串，2=packed listpack。返回拼接后的全部元素。
func readQuicklist(r *reader) ([]string, error) {
	n, special, _, err := readLen(r)
	if err != nil {
		return nil, err
	}
	if special {
		return nil, fmt.Errorf("rdb: quicklist count must not be special encoded")
	}
	var out []string
	for i := uint64(0); i < n; i++ {
		c, special, _, err := readLen(r)
		if err != nil {
			return nil, err
		}
		if special {
			return nil, fmt.Errorf("rdb: quicklist container must not be special encoded")
		}
		switch c {
		case 1:
			s, err := readString(r)
			if err != nil {
				return nil, err
			}
			out = append(out, string(s))
		case 2:
			blob, err := readString(r)
			if err != nil {
				return nil, err
			}
			ents, err := parseListpack(blob)
			if err != nil {
				return nil, err
			}
			out = append(out, ents...)
		default:
			return nil, fmt.Errorf("rdb: unknown quicklist container %d", c)
		}
	}
	return out, nil
}

// hllPackSet 按 dense 6-bit 打包置寄存器（镜像 datastruct.hllDenseSet，
// rdb 包不可见其未导出版本；12289 字节含 1 spare 供末寄存器越界读）。
func hllPackSet(reg []byte, pos int, val uint8) {
	b0 := pos * 6 / 8
	fb := uint(pos*6) & 7
	fb8 := 8 - fb
	v := uint(val)
	reg[b0] &= ^uint8(63 << fb)
	reg[b0] |= uint8(v << fb)
	if b0+1 < len(reg) {
		reg[b0+1] &= ^uint8(63 >> fb8)
		reg[b0+1] |= uint8(v >> fb8)
	}
}

// DensifySparseHLL 把 sparse HYLL blob 展开为 12289 字节 packed dense 寄存器。
// 操作码对标 hyperloglog.c：ZERO=00xxxxxx（(op&0x3f)+1 个 0）、
// XZERO=01xxxxxx+1扩展字节（(((op&0x3f)<<8)|ext)+1 个 0）、VAL=1vvvvvxx
// （值 ((op>>2)&0x1f)+1，共 (op&0x3)+1 个）。头 16 字节与 dense 同构。
func DensifySparseHLL(raw []byte) ([]byte, error) {
	if len(raw) < 16 || raw[0] != 'H' || raw[1] != 'Y' || raw[2] != 'L' || raw[3] != 'L' {
		return nil, fmt.Errorf("rdb: not a HyperLogLog blob")
	}
	if raw[4] != 1 {
		return nil, fmt.Errorf("rdb: not a sparse HLL (encoding %d)", raw[4])
	}
	p := raw[16:]
	reg := make([]byte, 12289)
	idx := 0
	for len(p) > 0 {
		op := p[0]
		switch {
		case op&0xC0 == 0:
			idx += int(op&0x3F) + 1
			p = p[1:]
		case op&0xC0 == 0x40:
			if len(p) < 2 {
				return nil, fmt.Errorf("rdb: truncated sparse HLL XZERO")
			}
			idx += (int(op&0x3F)<<8 | int(p[1])) + 1
			p = p[2:]
		default:
			val := ((op >> 2) & 0x1F) + 1
			n := int(op&0x3) + 1
			for i := 0; i < n; i++ {
				if idx >= 16384 {
					return nil, fmt.Errorf("rdb: sparse HLL overrun")
				}
				hllPackSet(reg, idx, val)
				idx++
			}
			p = p[1:]
		}
		if idx > 16384 {
			return nil, fmt.Errorf("rdb: sparse HLL overrun")
		}
	}
	if idx != 16384 {
		return nil, fmt.Errorf("rdb: sparse HLL covers %d registers, want 16384", idx)
	}
	return reg, nil
}
