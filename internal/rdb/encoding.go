// Package rdb 只读解析真机 RDB（pin RDB version 11 = Redis 7.2 输出），
// 把条目转成中性结构供导入器写入 Pebble。不做迁出方向。
package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
)

// ---------- length encoding ----------

// readLen 读 RDB 长度编码，返回 (长度, 是否特殊编码)。
// 特殊编码时返回的 waste 为特殊子类型（0=int8,1=int16,2=int32,3=lzf）。
func readLen(r *reader) (uint64, bool, byte, error) {
	b, err := r.u8()
	if err != nil {
		return 0, false, 0, err
	}
	switch b >> 6 {
	case 0:
		return uint64(b & 0x3f), false, 0, nil
	case 1:
		b2, err := r.u8()
		if err != nil {
			return 0, false, 0, err
		}
		return uint64(b&0x3f)<<8 | uint64(b2), false, 0, nil
	case 2:
		if b == 0x80 {
			v, err := r.u32be()
			if err != nil {
				return 0, false, 0, err
			}
			return uint64(v), false, 0, nil
		}
		if b == 0x81 {
			v, err := r.u64be()
			if err != nil {
				return 0, false, 0, err
			}
			return v, false, 0, nil
		}
		return 0, false, 0, fmt.Errorf("rdb: illegal length prefix %#02x", b)
	default:
		return 0, true, b & 0x3f, nil
	}
}

// ---------- string object ----------

// readString 读 RDB 字符串对象，整数/LZF 特殊编码统一展开为十进制/原文。
func readString(r *reader) ([]byte, error) {
	n, special, sub, err := readLen(r)
	if err != nil {
		return nil, err
	}
	if !special {
		return r.bytes(n)
	}
	switch sub {
	case 0:
		b, err := r.u8()
		if err != nil {
			return nil, err
		}
		return []byte(strconv.Itoa(int(int8(b)))), nil
	case 1:
		v, err := r.u16le()
		if err != nil {
			return nil, err
		}
		return []byte(strconv.Itoa(int(int16(v)))), nil
	case 2:
		v, err := r.u32le()
		if err != nil {
			return nil, err
		}
		return []byte(strconv.Itoa(int(int32(v)))), nil
	case 3:
		return readLZF(r)
	default:
		return nil, fmt.Errorf("rdb: unknown special string encoding %d", sub)
	}
}

// readLZF 读 LZF 压缩字符串：clen/ulen 均为长度编码，后跟压缩体。
func readLZF(r *reader) ([]byte, error) {
	clen, sc, _, err := readLen(r)
	if err != nil {
		return nil, err
	}
	if sc {
		return nil, fmt.Errorf("rdb: lzf clen must not be special encoded")
	}
	ulen, su, _, err := readLen(r)
	if err != nil {
		return nil, err
	}
	if su {
		return nil, fmt.Errorf("rdb: lzf ulen must not be special encoded")
	}
	comp, err := r.bytes(clen)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, ulen)
	i := 0
	for i < len(comp) {
		ctrl := comp[i]
		i++
		if ctrl < 32 {
			l := int(ctrl) + 1
			if i+l > len(comp) {
				return nil, fmt.Errorf("rdb: lzf literal overrun")
			}
			out = append(out, comp[i:i+l]...)
			i += l
			continue
		}
		length := int(ctrl >> 5)
		// extension 先行：offset 字节在长度扩展字节之后（lzf 格式序）。
		if length == 7 {
			for {
				if i >= len(comp) {
					return nil, fmt.Errorf("rdb: lzf len overrun")
				}
				length += int(comp[i])
				i++
				if comp[i-1] != 255 {
					break
				}
			}
		}
		if i >= len(comp) {
			return nil, fmt.Errorf("rdb: lzf match overrun")
		}
		ref := int(ctrl&0x1f)<<8 + int(comp[i]) + 1
		i++
		length += 2
		if ref > len(out) || ref < 1 {
			return nil, fmt.Errorf("rdb: lzf bad backref ref=%d out=%d ulen=%d", ref, len(out), ulen)
		}
		if uint64(len(out))+uint64(length) > ulen {
			return nil, fmt.Errorf("rdb: lzf overrun out=%d len=%d ulen=%d", len(out), length, ulen)
		}
		for j := 0; j < length; j++ {
			out = append(out, out[len(out)-ref])
		}
	}
	if uint64(len(out)) != ulen {
		return nil, fmt.Errorf("rdb: lzf length mismatch %d != %d", len(out), ulen)
	}
	return out, nil
}

// ---------- double ----------

// readDouble 读 zset classic 的文本 double（首字节为长度，
// 255/-inf 254/+inf 253/nan）。
func readDouble(r *reader) (float64, error) {
	l, err := r.u8()
	if err != nil {
		return 0, err
	}
	switch l {
	case 255:
		return math.Inf(-1), nil
	case 254:
		return math.Inf(1), nil
	case 253:
		return math.NaN(), nil
	}
	b, err := r.bytes(uint64(l))
	if err != nil {
		return 0, err
	}
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, fmt.Errorf("rdb: bad double %q", b)
	}
	return f, nil
}

// readDoubleBin 读 zset2 的 8 字节小端二进制 double。
func readDoubleBin(r *reader) (float64, error) {
	v, err := r.u64le()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

// ---------- crc64 (Redis 真机同款：reflected，初值 0，无异或。
// 以 fix-p0.rdb 尾部 checksum 为 ground truth 实测钉死。) ----------

var crc64tab [256]uint64

func init() {
	const poly = 0x95ac9329ac4bc9b5
	for i := 0; i < 256; i++ {
		crc := uint64(i)
		for j := 0; j < 8; j++ {
			if crc&1 == 1 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
		crc64tab[i] = crc
	}
}

func crc64(crc uint64, b []byte) uint64 {
	for _, v := range b {
		crc = crc64tab[byte(crc)^v] ^ (crc >> 8)
	}
	return crc
}

// ---------- byte reader (带 checksum 累加) ----------

type reader struct {
	rs  io.ReadSeeker
	sum uint64 // 已消费字节的 crc（不含尾部 8 字节 checksum，由调用方切分）
}

func newReader(rs io.ReadSeeker) *reader { return &reader{rs: rs} }

func (r *reader) u8() (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r.rs, b[:]); err != nil {
		return 0, fmt.Errorf("rdb: truncated: %w", err)
	}
	r.sum = crc64(r.sum, b[:])
	return b[0], nil
}

func (r *reader) bytes(n uint64) ([]byte, error) {
	if n > 1<<32 {
		return nil, fmt.Errorf("rdb: absurd length %d", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r.rs, b); err != nil {
		return nil, fmt.Errorf("rdb: truncated want %d: %w", n, err)
	}
	r.sum = crc64(r.sum, b)
	return b, nil
}

func (r *reader) u16le() (uint16, error) {
	b, err := r.bytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *reader) u32le() (uint32, error) {
	b, err := r.bytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *reader) u32be() (uint32, error) {
	b, err := r.bytes(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *reader) u64le() (uint64, error) {
	b, err := r.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *reader) u64be() (uint64, error) {
	b, err := r.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}
