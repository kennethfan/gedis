package rdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"time"
)

// Export 将条目编码为 RDB version 11 字节流，只写最简合法编码：plain
// string、经典容器编码（list1/set2/zset3/hash4）、HLL 原 HYLL blob、
// stream v3 单 rax 节点。不做 LZF/listpack 打包（文件偏大但真机可读），
// hash field 过期 sidecar 无 v11 编码故丢弃（偏差，见包文档）。
func Export(w io.Writer, entries []Entry) error {
	e := &exporter{}
	e.buf.WriteString("REDIS" + rdbVersionPin)
	e.aux("redis-ver", "7.4.0")
	e.aux("ctime", strconv.FormatInt(time.Now().Unix(), 10))
	e.u8(opSelectDB)
	e.writeLen(0)
	for _, en := range entries {
		if en.ExpireMs != 0 {
			e.u8(opExpireMs)
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(en.ExpireMs))
			e.buf.Write(b[:])
		}
		switch en.Type {
		case TypeString:
			e.u8(vtString)
			e.writeKey(en.Key)
			e.writeString(en.Str)
		case TypeList:
			e.u8(vtList)
			e.writeKey(en.Key)
			e.writeLen(uint64(len(en.List)))
			for _, s := range en.List {
				e.writeString([]byte(s))
			}
		case TypeSet:
			e.u8(vtSet)
			e.writeKey(en.Key)
			mems := sortedCopy(en.Set)
			e.writeLen(uint64(len(mems)))
			for _, s := range mems {
				e.writeString([]byte(s))
			}
		case TypeZSet:
			e.u8(vtZSet)
			e.writeKey(en.Key)
			ms := append([]MemberScore(nil), en.ZSet...)
			sort.Slice(ms, func(i, j int) bool { return ms[i].Member < ms[j].Member })
			e.writeLen(uint64(len(ms)))
			for _, m := range ms {
				e.writeString([]byte(m.Member))
				e.writeDouble(m.Score)
			}
		case TypeHash:
			e.u8(vtHash)
			e.writeKey(en.Key)
			fields := make([]string, 0, len(en.Hash))
			for f := range en.Hash {
				fields = append(fields, f)
			}
			sort.Strings(fields)
			e.writeLen(uint64(len(fields)))
			for _, f := range fields {
				e.writeString([]byte(f))
				e.writeString([]byte(en.Hash[f]))
			}
		case TypeHLL:
			if err := checkHLLBlob(en.HLL); err != nil {
				return err
			}
			e.u8(vtString)
			e.writeKey(en.Key)
			e.writeString(en.HLL)
		case TypeStream:
			e.u8(vtStreamLP3)
			e.writeKey(en.Key)
			if err := e.writeStream(en.Stream); err != nil {
				return err
			}
		default:
			return fmt.Errorf("rdb: cannot export type %q", en.Type)
		}
	}
	e.u8(opEOF)
	sum := crc64(0, e.buf.Bytes())
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], sum)
	e.buf.Write(b[:])
	_, err := w.Write(e.buf.Bytes())
	return err
}

type exporter struct {
	buf bytes.Buffer
}

func (e *exporter) u8(v byte) { e.buf.WriteByte(v) }

func (e *exporter) aux(k, v string) {
	e.u8(opAUX)
	e.writeString([]byte(k))
	e.writeString([]byte(v))
}

func (e *exporter) writeKey(k string) { e.writeString([]byte(k)) }

// writeLen 写 RDB 长度编码（6bit/14bit/32bit，与 readLen 镜像）。
func (e *exporter) writeLen(v uint64) {
	switch {
	case v < 64:
		e.u8(byte(v))
	case v < 16384:
		e.u8(byte(v>>8) | 0x40)
		e.u8(byte(v))
	case v < 4294967296:
		e.u8(0x80)
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(v))
		e.buf.Write(b[:])
	default:
		e.u8(0x81)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], v)
		e.buf.Write(b[:])
	}
}

// writeString 写 plain 字符串（不做 int/LZF 特殊编码，恒合法）。
func (e *exporter) writeString(b []byte) {
	e.writeLen(uint64(len(b)))
	e.buf.Write(b)
}

// writeDouble 写 zset 文本 double（与 readDouble 镜像：253/254/255
// 表 NaN/+Inf/-Inf，其余最短文本+长度前缀）。
func (e *exporter) writeDouble(f float64) {
	switch {
	case math.IsNaN(f):
		e.u8(253)
	case math.IsInf(f, 1):
		e.u8(254)
	case math.IsInf(f, -1):
		e.u8(255)
	default:
		s := strconv.FormatFloat(f, 'g', -1, 64)
		e.u8(byte(len(s)))
		e.buf.WriteString(s)
	}
}

// checkHLLBlob 守卫 HLL 导出：真机加载时校验 blob 合法性，非法会整库拒载。
// dense 须 16 头 + 12288 寄存器且 encoding=0；sparse 须 HYLL 开头且
// encoding=1（长度可变，只验头）。与 DecodeHLL 的真机同源文案对齐。
func checkHLLBlob(blob []byte) error {
	if len(blob) < 16 || blob[0] != 'H' || blob[1] != 'Y' || blob[2] != 'L' || blob[3] != 'L' {
		return fmt.Errorf("rdb: invalid HLL blob (%d bytes)", len(blob))
	}
	switch blob[4] {
	case 0:
		if len(blob) != 16+12288 {
			return fmt.Errorf("rdb: invalid dense HLL blob (%d bytes)", len(blob))
		}
	case 1:
	default:
		return fmt.Errorf("rdb: invalid HLL encoding %d", blob[4])
	}
	return nil
}

// ---------- listpack 写端（与 listpackEntry 解析镜像，条目布局见
// walkStreamListpack：master 为 count/deleted/名数/名/0；item 为
// flags/ms差/seq差/[numfields/名]/值/lp_count） ----------

func (e *exporter) lpUint(v uint64) {
	switch {
	case v <= 127:
		e.u8(byte(v))
	case v <= 4095:
		e.u8(byte(v>>8) | 0xc0)
		e.u8(byte(v))
	case v <= 32767:
		e.u8(0xf0)
		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], uint16(v))
		e.buf.Write(b[:])
	case v <= 2147483647:
		e.u8(0xf2)
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(v))
		e.buf.Write(b[:])
	default:
		e.u8(0xf3)
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], v)
		e.buf.Write(b[:])
	}
}

// lpInt 写有符号整数（13bit 负数走 raw+8192，与解析端镜像）。
func (e *exporter) lpInt(v int64) {
	switch {
	case v >= 0 && v <= 127:
		e.u8(byte(v))
	case v >= -4096 && v <= 4095:
		raw := uint64(v)
		if v < 0 {
			raw = uint64(v + 8192)
		}
		e.u8(byte(raw>>8) | 0xc0)
		e.u8(byte(raw))
	case v >= -32768 && v <= 32767:
		e.u8(0xf0)
		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], uint16(int16(v)))
		e.buf.Write(b[:])
	case v >= -2147483648 && v <= 2147483647:
		e.u8(0xf2)
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(int32(v)))
		e.buf.Write(b[:])
	default:
		e.u8(0xf3)
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(v))
		e.buf.Write(b[:])
	}
}

func (e *exporter) lpStr(s string) {
	switch n := len(s); {
	case n <= 63:
		e.u8(byte(n) | 0x80)
		e.buf.WriteString(s)
	case n <= 4095:
		e.u8(byte(n>>8) | 0xe0)
		e.u8(byte(n))
		e.buf.WriteString(s)
	default:
		e.u8(0xf4)
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(n))
		e.buf.Write(b[:])
		e.buf.WriteString(s)
	}
}

// lpBacklen 写回指长度：大端 7bit 组、末字节高位置位（sz>1 时），
// 单字节直接写值（与 parseListpack 回验镜像）。
func (e *exporter) lpBacklen(entryLen int) {
	sz := 1
	for v := entryLen; v > 127; v >>= 7 {
		sz++
	}
	for k := sz - 1; k >= 0; k-- {
		b := byte((entryLen >> (7 * k)) & 0x7f)
		if k == 0 && sz > 1 {
			b |= 0x80
		}
		e.u8(b)
	}
}

// lpEntry 写一个带 backlen 的 listpack 条目：记起点→调写回调→
// 按实际长度回指。
func (e *exporter) lpEntry(fn func()) {
	start := e.buf.Len()
	fn()
	e.lpBacklen(e.buf.Len() - start)
}

// buildStreamListpack 组 stream 单节点 listpack（含 6 字节头 + 0xFF 尾）。
func (e *exporter) buildStreamListpack(st *StreamData) ([]byte, error) {
	var inner exporter
	names := baseFieldNames(st)
	inner.lpEntry(func() { inner.lpUint(uint64(len(st.Entries))) })
	inner.lpEntry(func() { inner.lpUint(0) })
	inner.lpEntry(func() { inner.lpUint(uint64(len(names))) })
	for _, n := range names {
		inner.lpEntry(func() { inner.lpStr(n) })
	}
	inner.lpEntry(func() { inner.lpUint(0) })
	var baseMS, baseSeq uint64
	if len(st.Entries) > 0 {
		baseMS, baseSeq = st.Entries[0].MS, st.Entries[0].Seq
	}
	prev := names
	for _, en := range st.Entries {
		same := equalStrings(fieldNames(en.Fields), prev)
		inner.lpEntry(func() {
			if same {
				inner.lpUint(streamItemSameFields)
			} else {
				inner.lpUint(0)
			}
		})
		inner.lpEntry(func() { inner.lpInt(int64(en.MS) - int64(baseMS)) })
		inner.lpEntry(func() { inner.lpInt(int64(en.Seq) - int64(baseSeq)) })
		if !same {
			namesNow := fieldNames(en.Fields)
			inner.lpEntry(func() { inner.lpUint(uint64(len(namesNow))) })
			for _, n := range namesNow {
				inner.lpEntry(func() { inner.lpStr(n) })
			}
			prev = namesNow
		}
		for _, fv := range en.Fields {
			inner.lpEntry(func() { inner.lpStr(fv[1]) })
		}
		// lp_count：3+numfields（flags/ms/seq 三项+字段数），非 SAMEFIELDS
		// 另加 numfields+1（numfields 项+各字段名），与解析端回验镜像。
		nf := uint64(len(en.Fields))
		want := 3 + nf
		if !same {
			want += nf + 1
		}
		inner.lpEntry(func() { inner.lpUint(want) })
	}
	body := inner.buf.Bytes()
	nElem := countLPEntries(body)
	if nElem < 0 {
		return nil, fmt.Errorf("rdb: stream listpack recount failed")
	}
	out := make([]byte, 0, 6+len(body)+1)
	var hb [6]byte
	binary.LittleEndian.PutUint32(hb[0:4], uint32(6+len(body)+1))
	binary.LittleEndian.PutUint16(hb[4:6], uint16(nElem))
	out = append(out, hb[:]...)
	out = append(out, body...)
	out = append(out, 0xff)
	return out, nil
}

// baseFieldNames 取首条目字段名做 SAMEFIELDS 基（空流回空）。
func baseFieldNames(st *StreamData) []string {
	if len(st.Entries) == 0 {
		return nil
	}
	return fieldNames(st.Entries[0].Fields)
}

func fieldNames(fvs [][2]string) []string {
	out := make([]string, 0, len(fvs))
	for _, fv := range fvs {
		out = append(out, fv[0])
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// countLPEntries 数 body 中的条目数（复用解析端 backlen 定宽规则）。
func countLPEntries(body []byte) int {
	n, pos := 0, 0
	for pos < len(body) {
		entryStart := pos
		_, next, err := listpackEntry(body, pos, len(body))
		if err != nil {
			return -1
		}
		entryLen := next - entryStart
		sz := 1
		for v := entryLen; v > 127; v >>= 7 {
			sz++
		}
		pos = next + sz
		n++
	}
	return n
}

// writeStream 写 type21（v3）stream：单 rax 节点（首条目 ID 做 key）+
// 单 listpack + 全量 meta（inactive 消费者写 -1 哨兵）。
func (e *exporter) writeStream(st *StreamData) error {
	if st == nil {
		st = &StreamData{}
	}
	e.writeLen(1)
	var key [16]byte
	if len(st.Entries) > 0 {
		binary.BigEndian.PutUint64(key[0:8], st.Entries[0].MS)
		binary.BigEndian.PutUint64(key[8:16], st.Entries[0].Seq)
	}
		e.writeString(key[:])
		lp, err := e.buildStreamListpack(st)
		if err != nil {
			return err
		}
		e.writeString(lp)
	length := st.Length
	if length == 0 {
		length = uint64(len(st.Entries))
	}
	e.writeLen(length)
	lastMS, lastSeq := st.LastMS, st.LastSeq
	if len(st.Entries) > 0 && lastMS == 0 && lastSeq == 0 {
		lastMS = st.Entries[len(st.Entries)-1].MS
		lastSeq = st.Entries[len(st.Entries)-1].Seq
	}
	e.writeLen(lastMS)
	e.writeLen(lastSeq)
	firstMS, firstSeq := st.FirstMS, st.FirstSeq
	if len(st.Entries) > 0 && firstMS == 0 && firstSeq == 0 {
		firstMS, firstSeq = st.Entries[0].MS, st.Entries[0].Seq
	}
	e.writeLen(firstMS)
	e.writeLen(firstSeq)
	e.writeLen(st.MaxDeletedMS)
	e.writeLen(st.MaxDeletedSeq)
	added := st.Added
	if added < uint64(len(st.Entries)) {
		added = uint64(len(st.Entries))
	}
	e.writeLen(added)
	e.writeLen(uint64(len(st.Groups)))
	for _, g := range st.Groups {
		e.writeString([]byte(g.Name))
		e.writeLen(g.LastMS)
		e.writeLen(g.LastSeq)
		e.writeLen(g.EntriesRead)
		e.writeLen(uint64(len(g.Pending)))
		for _, p := range g.Pending {
			var id [16]byte
			binary.BigEndian.PutUint64(id[0:8], p.MS)
			binary.BigEndian.PutUint64(id[8:16], p.Seq)
			e.buf.Write(id[:])
			var t [8]byte
			binary.LittleEndian.PutUint64(t[:], p.DeliveryTime)
			e.buf.Write(t[:])
			e.writeLen(p.DeliveryCount)
		}
		e.writeLen(uint64(len(g.Consumers)))
		for _, c := range g.Consumers {
			e.writeString([]byte(c.Name))
			var t [8]byte
			binary.LittleEndian.PutUint64(t[:], c.SeenTime)
			e.buf.Write(t[:])
			active := c.ActiveTime
			if active == 0 {
				active = ^uint64(0)
			}
			binary.LittleEndian.PutUint64(t[:], active)
			e.buf.Write(t[:])
			var mine []StreamPending
			for _, p := range g.Pending {
				if p.Consumer == c.Name {
					mine = append(mine, p)
				}
			}
			e.writeLen(uint64(len(mine)))
			for _, p := range mine {
				var id [16]byte
				binary.BigEndian.PutUint64(id[0:8], p.MS)
				binary.BigEndian.PutUint64(id[8:16], p.Seq)
				e.buf.Write(id[:])
			}
		}
	}
	return nil
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
