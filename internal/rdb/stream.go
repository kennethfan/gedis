package rdb

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

// StreamEntry 是一条 stream 记录。
type StreamEntry struct {
	MS     uint64
	Seq    uint64
	Fields [][2]string
}

// StreamPending 是一条 PEL 记录。
type StreamPending struct {
	MS            uint64
	Seq           uint64
	DeliveryTime  uint64
	DeliveryCount uint64
	Consumer      string
}

// StreamConsumerData 是组内消费者快照。
type StreamConsumerData struct {
	Name       string
	SeenTime   uint64
	ActiveTime uint64
}

// StreamGroupData 是消费组快照。HasEntriesRead 标记 entries_read 是否来自
// RDB（仅 v2+ 有；v1 无该字段，导入时 HasRead=false）。
type StreamGroupData struct {
	Name           string
	LastMS         uint64
	LastSeq        uint64
	EntriesRead    uint64
	HasEntriesRead bool
	Pending        []StreamPending
	Consumers      []StreamConsumerData
}

// StreamData 是 type15/19/21 的完整解码。
type StreamData struct {
	Entries                           []StreamEntry
	Length                            uint64
	LastMS, LastSeq                   uint64
	FirstMS, FirstSeq                 uint64
	MaxDeletedMS, MaxDeletedSeq       uint64
	Added                             uint64
	Groups                            []StreamGroupData
}

const streamItemSameFields = 2

// readStream 读 stream。ver 为值类型字节（15/19/21）：v1 无
// first/max_deleted/added 与 entries_read，v1/v2 无 consumer active_time。
// PEL 条目 ID 为 16 原始字节（ms/seq 大端），时间为 8 原始字节小端，均非长度编码。
func readStream(r *reader, ver byte) (*StreamData, error) {
	n, special, _, err := readLen(r)
	if err != nil {
		return nil, err
	}
	if special {
		return nil, fmt.Errorf("rdb: stream listpack count must not be special encoded")
	}
	st := &StreamData{}
	var baseMS, baseSeq uint64
	for i := uint64(0); i < n; i++ {
		key, err := readString(r)
		if err != nil {
			return nil, err
		}
		if len(key) != 16 {
			return nil, fmt.Errorf("rdb: stream rax key len %d", len(key))
		}
		baseMS = binary.BigEndian.Uint64(key[0:8])
		baseSeq = binary.BigEndian.Uint64(key[8:16])
		blob, err := readString(r)
		if err != nil {
			return nil, err
		}
		toks, err := parseListpack(blob)
		if err != nil {
			return nil, err
		}
		ents, err := walkStreamListpack(toks)
		if err != nil {
			return nil, err
		}
		for j := range ents {
			// ID 差值相对 rax key（master），非逐条累加（streamIteratorGetID 语义）。
			st.Entries = append(st.Entries, StreamEntry{
				MS:     baseMS + ents[j].msDiff,
				Seq:    baseSeq + ents[j].seqDiff,
				Fields: ents[j].fields,
			})
		}
	}
	if err := readStreamMeta(r, st, ver); err != nil {
		return nil, err
	}
	return st, nil
}

type lpMasterEntry struct {
	fields         [][2]string
	msDiff, seqDiff uint64
}

// walkStreamListpack 走主条目（t_stream.c streamAppendItem 布局）：
// flags,ms差,seq差,[numfields,field名×n],value×n,lp_count。
// SAMEFIELDS 时省略 numfields+field 名（复用上条目）。
func walkStreamListpack(toks []string) ([]lpMasterEntry, error) {
	var out []lpMasterEntry
	var prevNames []string
	pos := 0
	atoi := func() (uint64, error) {
		if pos >= len(toks) {
			return 0, fmt.Errorf("rdb: stream listpack truncated")
		}
		v, err := strconv.ParseUint(toks[pos], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("rdb: stream listpack bad int %q", toks[pos])
		}
		pos++
		return v, nil
	}
	for pos < len(toks) {
		// 首段为 master 条目（count,deleted 无需使用）：取 field 名做 SAMEFIELDS 基，消费零终止。
		if pos == 0 {
			if _, err := atoi(); err != nil {
				return nil, err
			}
			if _, err := atoi(); err != nil {
				return nil, err
			}
			mn, err := atoi()
			if err != nil {
				return nil, err
			}
			for k := uint64(0); k < mn; k++ {
				if pos >= len(toks) {
					return nil, fmt.Errorf("rdb: stream listpack truncated master names")
				}
				prevNames = append(prevNames, toks[pos])
				pos++
			}
			zero, err := atoi()
			if err != nil {
				return nil, err
			}
			if zero != 0 {
				return nil, fmt.Errorf("rdb: stream master terminator %d", zero)
			}
			continue
		}
		flags, err := atoi()
		if err != nil {
			return nil, err
		}
		ms, err := atoi()
		if err != nil {
			return nil, err
		}
		seq, err := atoi()
		if err != nil {
			return nil, err
		}
		var names []string
		var numfields uint64
		if flags&streamItemSameFields != 0 {
			names = prevNames
			numfields = uint64(len(names))
			if numfields == 0 {
				return nil, fmt.Errorf("rdb: stream samefields without base")
			}
		} else {
			numfields, err = atoi()
			if err != nil {
				return nil, err
			}
			for k := uint64(0); k < numfields; k++ {
				if pos >= len(toks) {
					return nil, fmt.Errorf("rdb: stream listpack truncated names")
				}
				names = append(names, toks[pos])
				pos++
			}
			prevNames = names
		}
		var fields [][2]string
		for k := uint64(0); k < numfields; k++ {
			if pos >= len(toks) {
				return nil, fmt.Errorf("rdb: stream listpack truncated values")
			}
			fields = append(fields, [2]string{names[k], toks[pos]})
			pos++
		}
		lpCount, err := atoi()
		if err != nil {
			return nil, err
		}
		want := 3 + numfields
		if flags&streamItemSameFields == 0 {
			want += numfields + 1
		}
		if lpCount != want {
			return nil, fmt.Errorf("rdb: stream lp-count mismatch %d != %d", lpCount, want)
		}
		out = append(out, lpMasterEntry{fields: fields, msDiff: ms, seqDiff: seq})
	}
	return out, nil
}

// readMetaUint 读 stream 元信息整数：普通长度编码 + int8/16/32 特殊编码（真机
// last_id ms 等稍大整数即走 int 特殊编码），LZF 拒绝。
func readMetaUint(r *reader) (uint64, error) {
	v, special, sub, err := readLen(r)
	if err != nil {
		return 0, err
	}
	if !special {
		return v, nil
	}
	switch sub {
	case 0:
		b, err := r.u8()
		if err != nil {
			return 0, err
		}
		return uint64(int64(int8(b))), nil
	case 1:
		u, err := r.u16le()
		if err != nil {
			return 0, err
		}
		return uint64(int64(int16(u))), nil
	case 2:
		u, err := r.u32le()
		if err != nil {
			return 0, err
		}
		return uint64(int64(int32(u))), nil
	default:
		return 0, fmt.Errorf("rdb: stream meta must not be lzf encoded")
	}
}

// readStreamMeta 读 listpack 之后：length,last,first(v2+),max_deleted(v2+),
// added(v2+),groups+PEL+consumers。raw16 读 PEL 条目 ID（大端 ms/seq），
// raw64le 读毫秒时刻。
func readStreamMeta(r *reader, st *StreamData, ver byte) error {
	v2 := ver >= vtStreamLP2
	v3 := ver >= vtStreamLP3
	raw16 := func() (uint64, uint64, error) {
		b, err := r.bytes(16)
		if err != nil {
			return 0, 0, err
		}
		return binary.BigEndian.Uint64(b[0:8]), binary.BigEndian.Uint64(b[8:16]), nil
	}
	raw64le := func() (uint64, error) {
		b, err := r.bytes(8)
		if err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(b), nil
	}
	nlu := 0
	lu := func() (uint64, error) {
		nlu++
		v, err := readMetaUint(r)
		if err != nil {
			return 0, fmt.Errorf("meta#%d: %w", nlu, err)
		}
		return v, nil
	}
	var err error
	if st.Length, err = lu(); err != nil {
		return err
	}
	if st.LastMS, err = lu(); err != nil {
		return err
	}
	if st.LastSeq, err = lu(); err != nil {
		return err
	}
	if v2 {
		if st.FirstMS, err = lu(); err != nil {
			return err
		}
		if st.FirstSeq, err = lu(); err != nil {
			return err
		}
		if st.MaxDeletedMS, err = lu(); err != nil {
			return err
		}
		if st.MaxDeletedSeq, err = lu(); err != nil {
			return err
		}
		if st.Added, err = lu(); err != nil {
			return err
		}
	} else if len(st.Entries) > 0 {
		st.FirstMS, st.FirstSeq = st.Entries[0].MS, st.Entries[0].Seq
		st.Added = st.Length
	}
	ng, err := lu()
	if err != nil {
		return err
	}
	for i := uint64(0); i < ng; i++ {
		g := StreamGroupData{}
		name, err := readString(r)
		if err != nil {
			return err
		}
		g.Name = string(name)
		if g.LastMS, err = lu(); err != nil {
			return err
		}
		if g.LastSeq, err = lu(); err != nil {
			return err
		}
		if v2 {
			if g.EntriesRead, err = lu(); err != nil {
				return err
			}
			g.HasEntriesRead = true
		}
		npel, err := lu()
		if err != nil {
			return err
		}
		for k := uint64(0); k < npel; k++ {
			p := StreamPending{}
			if p.MS, p.Seq, err = raw16(); err != nil {
				return err
			}
			if p.DeliveryTime, err = raw64le(); err != nil {
				return err
			}
			if p.DeliveryCount, err = lu(); err != nil {
				return err
			}
			g.Pending = append(g.Pending, p)
		}
		nc, err := lu()
		if err != nil {
			return err
		}
		owner := map[[2]uint64]string{}
		for k := uint64(0); k < nc; k++ {
			c := StreamConsumerData{}
			cn, err := readString(r)
			if err != nil {
				return err
			}
			c.Name = string(cn)
			if c.SeenTime, err = raw64le(); err != nil {
				return err
			}
			if v3 {
				if c.ActiveTime, err = raw64le(); err != nil {
					return err
				}
		} else {
			// 非 v3 无 active_time：填真机 -1 哨兵（raw64le 全 1），导入时
			// 映射为 HasActive=false。填 SeenTime 会误判为“有过投递”。
			c.ActiveTime = ^uint64(0)
		}
			ncp, err := lu()
			if err != nil {
				return err
			}
			for j := uint64(0); j < ncp; j++ {
				ms, seq, err := raw16()
				if err != nil {
					return err
				}
				owner[[2]uint64{ms, seq}] = c.Name
			}
			g.Consumers = append(g.Consumers, c)
		}
		for k := range g.Pending {
			p := &g.Pending[k]
			p.Consumer = owner[[2]uint64{p.MS, p.Seq}]
		}
		st.Groups = append(st.Groups, g)
	}
	return nil
}
