package rdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// RDB opcode。
const (
	opAUX         = 0xFA
	opResizeDB    = 0xFB
	opExpireMs    = 0xFC
	opExpireSec   = 0xFD
	opSelectDB    = 0xFE
	opEOF         = 0xFF
	rdbVersionPin = "0011" // Redis 7.2 输出，非此版本拒载。
)

// RDB value type（写盘类型）。
const (
	vtString       = 0
	vtList         = 1
	vtSet          = 2
	vtZSet         = 3
	vtHash         = 4
	vtZSet2        = 5
	vtModule       = 6
	vtModule2      = 7
	vtHashZipmap   = 9
	vtListZiplist  = 10
	vtSetIntset    = 11
	vtZSetZiplist  = 12
	vtHashZiplist  = 13
	vtListQuick    = 14
	vtStreamLP     = 15
	vtHashListpack = 16
	vtZSetListpack = 17
	vtListQuick2   = 18
	vtStreamLP2    = 19
	vtSetListpack  = 20
	vtStreamLP3    = 21
)

// ValueType 是解析后的逻辑类型。
type ValueType string

const (
	TypeString ValueType = "string"
	TypeList   ValueType = "list"
	TypeSet    ValueType = "set"
	TypeZSet   ValueType = "zset"
	TypeHash   ValueType = "hash"
	TypeStream ValueType = "stream"
	TypeHLL    ValueType = "hll"
)

// MemberScore 是 zset 成员。
type MemberScore struct {
	Member string
	Score  float64
}

// Entry 是一条解析后的键值条目。ExpireMs 为绝对 unix 毫秒，0 表无过期。
type Entry struct {
	Key      string
	Type     ValueType
	ExpireMs int64
	Str      []byte
	HLL      []byte // TypeHLL 时为 HYLL 原 blob
	List     []string
	Set      []string
	ZSet     []MemberScore
	Hash     map[string]string
	Stream   *StreamData
}

// Skipped 记录解析时跳过的键（stream 待 P1、module 永不支持）。
type Skipped struct {
	Key    string
	Reason string
}

// Result 是 Load 的完整结果。
type Result struct {
	Entries []Entry
	Skipped []Skipped
}

// LoadFile 从路径加载 RDB。
func LoadFile(path string) (*Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("rdb: read %s: %w", path, err)
	}
	return Load(bytes.NewReader(data))
}

// Load 解析 RDB 流。版本非 0011、checksum 不匹配直接失败。
func Load(rs io.ReadSeeker) (*Result, error) {
	// 尾部 8 字节为 CRC64：先验长度，再把 reader 限定在 checksum 之前。
	end, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("rdb: seek: %w", err)
	}
	if end < 9+8 {
		return nil, fmt.Errorf("rdb: file too short %d", end)
	}
	want := make([]byte, 8)
	if _, err := rs.Seek(end-8, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rdb: seek: %w", err)
	}
	if _, err := io.ReadFull(rs, want); err != nil {
		return nil, fmt.Errorf("rdb: read checksum: %w", err)
	}
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rdb: seek: %w", err)
	}
	body, err := io.ReadAll(io.LimitReader(rs, end-8))
	if err != nil {
		return nil, fmt.Errorf("rdb: read body: %w", err)
	}
	if c := crc64(0, body); !equalU64LE(c, want) {
		return nil, fmt.Errorf("rdb: checksum mismatch (not a Redis RDB or corrupted)")
	}
	r := newReader(bytes.NewReader(body))
	res := &Result{}
	if err := parse(r, res); err != nil {
		return nil, err
	}
	return res, nil
}

func crcOf(b []byte) uint64 { return crc64(0, b) }

func equalU64LE(v uint64, b []byte) bool {
	for i := 0; i < 8; i++ {
		if byte(v>>(8*i)) != b[i] {
			return false
		}
	}
	return true
}

func parse(r *reader, res *Result) error {
	magic, err := r.bytes(5)
	if err != nil {
		return err
	}
	if string(magic) != "REDIS" {
		return fmt.Errorf("rdb: bad magic %q", magic)
	}
	ver, err := r.bytes(4)
	if err != nil {
		return err
	}
	if string(ver) != rdbVersionPin {
		return fmt.Errorf("rdb: unsupported version %s (support %s only)", ver, rdbVersionPin)
	}
	db := -1
	var expireMs int64
	for {
		op, err := r.u8()
		if err != nil {
			return err
		}
		switch op {
		case opAUX:
			if _, err := readString(r); err != nil {
				return err
			}
			if _, err := readString(r); err != nil {
				return err
			}
		case opResizeDB:
			if _, _, _, err := readLen(r); err != nil {
				return err
			}
			if _, _, _, err := readLen(r); err != nil {
				return err
			}
		case opExpireMs:
			ms, err := r.u64le()
			if err != nil {
				return err
			}
			expireMs = int64(ms)
		case opExpireSec:
			b4, err := r.bytes(4)
			if err != nil {
				return err
			}
			expireMs = int64(binary.LittleEndian.Uint32(b4)) * 1000
		case opSelectDB:
			n, special, _, err := readLen(r)
			if err != nil {
				return err
			}
			if special {
				return fmt.Errorf("rdb: selectdb must not be special encoded")
			}
			db = int(n)
		case opEOF:
			return nil
		default:
			if db != 0 {
				return fmt.Errorf("rdb: multi-DB file (db=%d): only DB 0 importable, refusing", db)
			}
			if err := parseValue(r, res, op, expireMs); err != nil {
				return fmt.Errorf("rdb: db=%d: %w", db, err)
			}
			expireMs = 0
		}
	}
}

// parseValue 解析一条 [valuetype key value]，op 即 valuetype 字节。
func parseValue(r *reader, res *Result, op byte, expireMs int64) error {
	kb, err := readString(r)
	if err != nil {
		return err
	}
	key := string(kb)
	e := Entry{Key: key, ExpireMs: expireMs}
	switch op {
	case vtString:
		s, err := readString(r)
		if err != nil {
			return err
		}
		if isHYLL(s) {
			e.Type, e.HLL = TypeHLL, s
		} else {
			e.Type, e.Str = TypeString, s
		}
	case vtList:
		n, special, _, err := readLen(r)
		if err != nil {
			return err
		}
		if special {
			return fmt.Errorf("rdb: list count must not be special encoded")
		}
		e.Type = TypeList
		for i := uint64(0); i < n; i++ {
			s, err := readString(r)
			if err != nil {
				return err
			}
			e.List = append(e.List, string(s))
		}
	case vtSet:
		n, special, _, err := readLen(r)
		if err != nil {
			return err
		}
		if special {
			return fmt.Errorf("rdb: set count must not be special encoded")
		}
		e.Type = TypeSet
		for i := uint64(0); i < n; i++ {
			s, err := readString(r)
			if err != nil {
				return err
			}
			e.Set = append(e.Set, string(s))
		}
	case vtZSet:
		n, special, _, err := readLen(r)
		if err != nil {
			return err
		}
		if special {
			return fmt.Errorf("rdb: zset count must not be special encoded")
		}
		e.Type = TypeZSet
		for i := uint64(0); i < n; i++ {
			m, err := readString(r)
			if err != nil {
				return err
			}
			f, err := readDouble(r)
			if err != nil {
				return err
			}
			e.ZSet = append(e.ZSet, MemberScore{Member: string(m), Score: f})
		}
	case vtHash:
		n, special, _, err := readLen(r)
		if err != nil {
			return err
		}
		if special {
			return fmt.Errorf("rdb: hash count must not be special encoded")
		}
		e.Type = TypeHash
		e.Hash = make(map[string]string, n)
		for i := uint64(0); i < n; i++ {
			f, err := readString(r)
			if err != nil {
				return err
			}
			v, err := readString(r)
			if err != nil {
				return err
			}
			e.Hash[string(f)] = string(v)
		}
	case vtZSet2:
		n, special, _, err := readLen(r)
		if err != nil {
			return err
		}
		if special {
			return fmt.Errorf("rdb: zset2 count must not be special encoded")
		}
		e.Type = TypeZSet
		for i := uint64(0); i < n; i++ {
			m, err := readString(r)
			if err != nil {
				return err
			}
			f, err := readDoubleBin(r)
			if err != nil {
				return err
			}
			e.ZSet = append(e.ZSet, MemberScore{Member: string(m), Score: f})
		}
	case vtSetIntset:
		blob, err := readString(r)
		if err != nil {
			return err
		}
		mems, err := parseIntset(blob)
		if err != nil {
			return err
		}
		e.Type, e.Set = TypeSet, mems
	case vtHashListpack, vtZSetListpack, vtSetListpack:
		blob, err := readString(r)
		if err != nil {
			return err
		}
		ents, err := parseListpack(blob)
		if err != nil {
			return err
		}
		switch op {
		case vtHashListpack:
			if len(ents)%2 != 0 {
				return fmt.Errorf("rdb: hash listpack odd entries %d", len(ents))
			}
			e.Type = TypeHash
			e.Hash = make(map[string]string, len(ents)/2)
			for i := 0; i < len(ents); i += 2 {
				e.Hash[ents[i]] = ents[i+1]
			}
		case vtZSetListpack:
			if len(ents)%2 != 0 {
				return fmt.Errorf("rdb: zset listpack odd entries %d", len(ents))
			}
			e.Type = TypeZSet
			for i := 0; i < len(ents); i += 2 {
				f, err := parseListpackDouble(ents[i+1])
				if err != nil {
					return err
				}
				e.ZSet = append(e.ZSet, MemberScore{Member: ents[i], Score: f})
			}
		default:
			e.Type, e.Set = TypeSet, ents
		}
	case vtListQuick, vtListQuick2:
		elems, err := readQuicklist(r)
		if err != nil {
			return err
		}
		e.Type, e.List = TypeList, elems
	case vtStreamLP, vtStreamLP2, vtStreamLP3:
		st, err := readStream(r, op)
		if err != nil {
			return err
		}
		e.Type, e.Stream = TypeStream, st
	case vtModule, vtModule2:
		return fmt.Errorf("rdb: key %q uses module-assisted type, cannot import", key)
	default:
		return fmt.Errorf("rdb: unsupported value type %d for key %q", op, key)
	}
	res.Entries = append(res.Entries, e)
	return nil
}

// isHYLL 识别 HLL（真机以 string 存，HYLL 魔头）。
func isHYLL(s []byte) bool {
	return len(s) >= 4 && s[0] == 'H' && s[1] == 'Y' && s[2] == 'L' && s[3] == 'L'
}
