package datastruct

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// StreamKey 给用户 key 加 x: 前缀。
func StreamKey(key string) []byte { return append([]byte("x:"), key...) }

// StreamID 是 entry-id（ms-seq），比较按 (ms, seq) 字典序。
type StreamID struct {
	Ms  uint64
	Seq uint64
}

func (id StreamID) Less(o StreamID) bool {
	if id.Ms != o.Ms {
		return id.Ms < o.Ms
	}
	return id.Seq < o.Seq
}

func (id StreamID) Compare(o StreamID) int {
	switch {
	case id.Less(o):
		return -1
	case o.Less(id):
		return 1
	}
	return 0
}

func (id StreamID) String() string {
	return strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Seq, 10)
}

// StreamParseID 解析严格 ID 形："*"（全自动）、"ms-*"（seq 自动）、
// "ms-seq"、裸 "ms"（即 ms-0，对标真 Redis）。"-"、"+"、"$"、">" 由命令层映射。
func StreamParseID(s string) (StreamID, bool, error) {
	invalid := fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	if s == "*" {
		return StreamID{}, true, nil
	}
	msStr, seqStr, ok := strings.Cut(s, "-")
	if !ok {
		ms, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return StreamID{}, false, invalid
		}
		return StreamID{Ms: ms}, false, nil
	}
	ms, err := strconv.ParseUint(msStr, 10, 64)
	if err != nil {
		return StreamID{}, false, invalid
	}
	if seqStr == "*" {
		return StreamID{Ms: ms}, true, nil
	}
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil || strings.Contains(seqStr, "-") {
		return StreamID{}, false, invalid
	}
	return StreamID{Ms: ms, Seq: seq}, false, nil
}

// StreamMinID / StreamMaxID 对应 XRANGE 的 "-" / "+" 边界。
var (
	StreamMinID = StreamID{Ms: 0, Seq: 0}
	StreamMaxID = StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}
)

// StreamEntry 是一条 stream 记录：ID + 保序 field/value 对（扁平存放）。
type StreamEntry struct {
	ID     StreamID
	Fields []string
}

// StreamPEL 是一条 pending 记录：entry-id、属主消费者、最近投递时刻 ms、投递次数。
type StreamPEL struct {
	ID         StreamID
	Consumer   string
	DeliveryMs uint64
	Count      uint64
}

// StreamConsumer 是组内消费者：名、首次出现时刻 seen-ms、
// 最近一次 PEL 投递时刻（无投递时 HasActive=false，对应 inactive=-1）。
type StreamConsumer struct {
	Name      string
	SeenMs    uint64
	ActiveMs  uint64
	HasActive bool
}

// StreamGroup 是消费组：最后投递 ID、已读计数、消费者表、PEL。
type StreamGroup struct {
	Name        string
	LastID      StreamID
	EntriesRead uint64
	HasRead     bool
	Consumers   []StreamConsumer
	PEL         []*StreamPEL
}

// FindConsumer 按名找消费者，命中返回下标，未命中返回 -1。
func (g *StreamGroup) FindConsumer(name string) int {
	for i, c := range g.Consumers {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// EnsureConsumer 取或建消费者；新建时 SeenMs=now，不设 active。返回下标。
func (g *StreamGroup) EnsureConsumer(name string, nowMs uint64) int {
	if i := g.FindConsumer(name); i >= 0 {
		return i
	}
	g.Consumers = append(g.Consumers, StreamConsumer{Name: name, SeenMs: nowMs})
	return len(g.Consumers) - 1
}

// FindPEL 按 ID 找 pending 记录，未命中返回 nil。
func (g *StreamGroup) FindPEL(id StreamID) *StreamPEL {
	for _, p := range g.PEL {
		if p.ID == id {
			return p
		}
	}
	return nil
}

// UpsertPEL 插入或替换 pending 记录，保持 PEL 按 ID 升序。
func (g *StreamGroup) UpsertPEL(p *StreamPEL) {
	for i, e := range g.PEL {
		if e.ID == p.ID {
			g.PEL[i] = p
			return
		}
		if p.ID.Less(e.ID) {
			g.PEL = append(g.PEL, nil)
			copy(g.PEL[i+1:], g.PEL[i:])
			g.PEL[i] = p
			return
		}
	}
	g.PEL = append(g.PEL, p)
}

// DelPEL 删除指定 ID 的 pending 记录，命中返回 true。
func (g *StreamGroup) DelPEL(id StreamID) bool {
	for i, e := range g.PEL {
		if e.ID == id {
			g.PEL = append(g.PEL[:i], g.PEL[i+1:]...)
			return true
		}
	}
	return false
}

// Stream 是内存中的 stream 全量：有序 entries + last-id + entries-added +
// max-deleted + 建制 groups。
type Stream struct {
	Entries    []StreamEntry
	Last       StreamID
	HasLast    bool
	Added      uint64
	MaxDeleted StreamID
	Groups     []*StreamGroup
}

func StreamNew() *Stream { return &Stream{} }

func (s *Stream) Add(id StreamID, fields []string) {
	s.Entries = append(s.Entries, StreamEntry{ID: id, Fields: fields})
}

func (s *Stream) Top() (StreamID, bool) {
	if len(s.Entries) == 0 {
		return StreamID{}, false
	}
	return s.Entries[len(s.Entries)-1].ID, true
}

// EncodeStream 编码全量：[uvarint n][entries: ms,seq,uvarint nfields,fields…]
// [lastMs,lastSeq,hasLast][added][maxDelMs,maxDelSeq][uvarint ngroups][groups…]。
func EncodeStream(s *Stream) []byte {
	size := 2 * binary.MaxVarintLen64
	for _, e := range s.Entries {
		size += 3*binary.MaxVarintLen64
		for _, f := range e.Fields {
			size += binary.MaxVarintLen64 + len(f)
		}
	}
	for _, g := range s.Groups {
		size += binary.MaxVarintLen64 + len(g.Name) + 4*binary.MaxVarintLen64 + 1
		for _, c := range g.Consumers {
			size += 3*binary.MaxVarintLen64 + 1 + len(c.Name)
		}
		for _, p := range g.PEL {
			size += 5*binary.MaxVarintLen64 + len(p.Consumer)
		}
	}
	out := make([]byte, 0, size)
	out = binary.AppendUvarint(out, uint64(len(s.Entries)))
	for _, e := range s.Entries {
		out = binary.AppendUvarint(out, e.ID.Ms)
		out = binary.AppendUvarint(out, e.ID.Seq)
		out = binary.AppendUvarint(out, uint64(len(e.Fields)))
		for _, f := range e.Fields {
			out = binary.AppendUvarint(out, uint64(len(f)))
			out = append(out, f...)
		}
	}
	out = binary.AppendUvarint(out, s.Last.Ms)
	out = binary.AppendUvarint(out, s.Last.Seq)
	if s.HasLast {
		out = binary.AppendUvarint(out, 1)
	} else {
		out = binary.AppendUvarint(out, 0)
	}
	out = binary.AppendUvarint(out, s.Added)
	out = binary.AppendUvarint(out, s.MaxDeleted.Ms)
	out = binary.AppendUvarint(out, s.MaxDeleted.Seq)
	out = binary.AppendUvarint(out, uint64(len(s.Groups)))
	for _, g := range s.Groups {
		out = binary.AppendUvarint(out, uint64(len(g.Name)))
		out = append(out, g.Name...)
		out = binary.AppendUvarint(out, g.LastID.Ms)
		out = binary.AppendUvarint(out, g.LastID.Seq)
		if g.HasRead {
			out = binary.AppendUvarint(out, 1)
		} else {
			out = binary.AppendUvarint(out, 0)
		}
		out = binary.AppendUvarint(out, g.EntriesRead)
		out = binary.AppendUvarint(out, uint64(len(g.Consumers)))
		for _, c := range g.Consumers {
			out = binary.AppendUvarint(out, uint64(len(c.Name)))
			out = append(out, c.Name...)
			out = binary.AppendUvarint(out, c.SeenMs)
			if c.HasActive {
				out = binary.AppendUvarint(out, 1)
			} else {
				out = binary.AppendUvarint(out, 0)
			}
			out = binary.AppendUvarint(out, c.ActiveMs)
		}
		out = binary.AppendUvarint(out, uint64(len(g.PEL)))
		for _, p := range g.PEL {
			out = binary.AppendUvarint(out, p.ID.Ms)
			out = binary.AppendUvarint(out, p.ID.Seq)
			out = binary.AppendUvarint(out, uint64(len(p.Consumer)))
			out = append(out, p.Consumer...)
			out = binary.AppendUvarint(out, p.DeliveryMs)
			out = binary.AppendUvarint(out, p.Count)
		}
	}
	return out
}

// DecodeStream 解析 EncodeStream 的输出，字段数为奇数或截断时报错。
func DecodeStream(raw []byte) (*Stream, error) {
	rest := raw
	bad := func() (*Stream, error) { return nil, fmt.Errorf("datastruct: bad stream payload") }
	n, adv := binary.Uvarint(rest)
	if adv <= 0 {
		return bad()
	}
	rest = rest[adv:]
	s := StreamNew()
	for i := uint64(0); i < n; i++ {
		var e StreamEntry
		var nf uint64
		var ok bool
		if e.ID.Ms, rest, ok = readUvarint(rest); !ok {
			return bad()
		}
		if e.ID.Seq, rest, ok = readUvarint(rest); !ok {
			return bad()
		}
		if nf, rest, ok = readUvarint(rest); !ok {
			return bad()
		}
		for j := uint64(0); j < nf; j++ {
			var f string
			if f, rest, ok = readStreamStr(rest); !ok {
				return bad()
			}
			e.Fields = append(e.Fields, f)
		}
		if len(e.Fields)%2 != 0 {
			return bad()
		}
		s.Entries = append(s.Entries, e)
	}
	if s.Last.Ms, rest, _ = readUvarint(rest); rest == nil {
		return bad()
	}
	if s.Last.Seq, rest, _ = readUvarint(rest); rest == nil {
		return bad()
	}
	var hasLast uint64
	var ok bool
	if hasLast, rest, ok = readUvarint(rest); !ok {
		return bad()
	}
	s.HasLast = hasLast != 0
	if s.Added, rest, ok = readUvarint(rest); !ok {
		return bad()
	}
	if s.MaxDeleted.Ms, rest, ok = readUvarint(rest); !ok {
		return bad()
	}
	if s.MaxDeleted.Seq, rest, ok = readUvarint(rest); !ok {
		return bad()
	}
	var ng uint64
	if ng, rest, ok = readUvarint(rest); !ok {
		return bad()
	}
	for i := uint64(0); i < ng; i++ {
		g := &StreamGroup{}
		if g.Name, rest, ok = readStreamStr(rest); !ok {
			return bad()
		}
		if g.LastID.Ms, rest, ok = readUvarint(rest); !ok {
			return bad()
		}
		if g.LastID.Seq, rest, ok = readUvarint(rest); !ok {
			return bad()
		}
		var hr uint64
		if hr, rest, ok = readUvarint(rest); !ok {
			return bad()
		}
		g.HasRead = hr != 0
		if g.EntriesRead, rest, ok = readUvarint(rest); !ok {
			return bad()
		}
		var nc uint64
		if nc, rest, ok = readUvarint(rest); !ok {
			return bad()
		}
		for j := uint64(0); j < nc; j++ {
			var c StreamConsumer
			if c.Name, rest, ok = readStreamStr(rest); !ok {
				return bad()
			}
			if c.SeenMs, rest, ok = readUvarint(rest); !ok {
				return bad()
			}
			var ha uint64
			if ha, rest, ok = readUvarint(rest); !ok {
				return bad()
			}
			c.HasActive = ha != 0
			if c.ActiveMs, rest, ok = readUvarint(rest); !ok {
				return bad()
			}
			g.Consumers = append(g.Consumers, c)
		}
		var np uint64
		if np, rest, ok = readUvarint(rest); !ok {
			return bad()
		}
		for j := uint64(0); j < np; j++ {
			p := &StreamPEL{}
			if p.ID.Ms, rest, ok = readUvarint(rest); !ok {
				return bad()
			}
			if p.ID.Seq, rest, ok = readUvarint(rest); !ok {
				return bad()
			}
			if p.Consumer, rest, ok = readStreamStr(rest); !ok {
				return bad()
			}
			if p.DeliveryMs, rest, ok = readUvarint(rest); !ok {
				return bad()
			}
			if p.Count, rest, ok = readUvarint(rest); !ok {
				return bad()
			}
			g.PEL = append(g.PEL, p)
		}
		s.Groups = append(s.Groups, g)
	}
	return s, nil
}

func readUvarint(b []byte) (uint64, []byte, bool) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 {
		return 0, b, false
	}
	return n, b[adv:], true
}

func readStreamStr(b []byte) (string, []byte, bool) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 || uint64(len(b[adv:])) < n {
		return "", b, false
	}
	return string(b[adv : adv+int(n)]), b[adv+int(n):], true
}
