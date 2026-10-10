package rdb

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/storage"
)

// memBatch 是内存假槽：收 WriteBatch 的 ops。
type memBatch struct {
	ops []storage.BatchOp
}

func (m *memBatch) WriteBatch(_ context.Context, ops []storage.BatchOp) error {
	m.ops = append(m.ops, ops...)
	return nil
}

func loadFixture(t *testing.T) *Result {
	t.Helper()
	res, err := LoadFile("testdata/fix-p0.rdb")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	return res
}

// T1 fixture 全量导入：11 键全部落槽，存储单元可解码。
func Test_Import_when_Fixture(t *testing.T) {
	res := loadFixture(t)
	// 内容断言不受 fixture TTL 腐烂影响：清掉过期再导（过期语义由 T1b/T2 覆盖）。
	for i := range res.Entries {
		res.Entries[i].ExpireMs = 0
	}
	mb := &memBatch{}
	st, err := Import(context.Background(), res, mb)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if st.Loaded != 11 || st.ExpiredSkipped != 0 {
		t.Fatalf("stats=%+v, want Loaded=11 ExpiredSkipped=0", st)
	}
	if len(mb.ops) != 11 {
		t.Fatalf("ops=%d, want 11", len(mb.ops))
	}
	got := map[string][]byte{}
	for _, op := range mb.ops {
		if op.Delete {
			t.Fatalf("unexpected delete op for %q", op.Key)
		}
		got[string(op.Key)] = op.Value
	}
	// T1a string 无过期：头 type+0。
	ent, err := datastruct.Decode(got["s:str0"])
	if err != nil {
		t.Fatalf("decode str0: %v", err)
	}
	if ent.Type != datastruct.TypeString || ent.Expiry != 0 || string(ent.Payload) != "hello" {
		t.Fatalf("str0=%+v payload=%q", ent, ent.Payload)
	}
	// T1b 过期换算：合成未来过期条目，头 expiry == ms*1e6。
	// 不用 fixture 值：fixture TTL 是相对时间，pin 绝对值会随日期腐烂。
	futureMs := time.Now().UnixNano()/1e6 + 3600*1000
	res2 := &Result{Entries: []Entry{{Key: "e", Type: TypeString, Str: []byte("v"), ExpireMs: futureMs}}}
	mb2 := &memBatch{}
	if _, err := Import(context.Background(), res2, mb2); err != nil {
		t.Fatalf("Import: %v", err)
	}
	ent2, err := datastruct.Decode(mb2.ops[0].Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ent2.Expiry != futureMs*1e6 {
		t.Fatalf("expiry=%d, want %d", ent2.Expiry, futureMs*1e6)
	}
	// T1c hash 内容：DecodeHash 还原 3 field。
	ent, _ = datastruct.Decode(got["h:h0"])
	hm, _, err := datastruct.DecodeHash(ent.Payload)
	if err != nil {
		t.Fatalf("DecodeHash: %v", err)
	}
	if len(hm) != 2 || hm["f1"] != "v1" || hm["f2"] != "v2" {
		t.Fatalf("hash=%v", hm)
	}
	// T1d zset 分数：bigz200m 抽查。
	ent, _ = datastruct.Decode(got["z:bigz"])
	zs, _, err := datastruct.DecodeZSet(ent.Payload)
	if err != nil {
		t.Fatalf("DecodeZSet: %v", err)
	}
	if len(zs) != 200 || zs["m200"] != 200 {
		t.Fatalf("zset size=%d m200=%v", len(zs), zs["m200"])
	}
	// T1e intset 还原：50 个十进制串。
	ent, _ = datastruct.Decode(got["st:intset0"])
	sm, _, err := datastruct.DecodeSet(ent.Payload)
	if err != nil {
		t.Fatalf("DecodeSet: %v", err)
	}
	if len(sm) != 50 {
		t.Fatalf("intset size=%d", len(sm))
	}
	// T1f sparse HLL densify：fixture hll0 经独立脚本核验只有 3 个非零寄存器
	//（小集合），导入后估计值应为个位数。
	ent, _ = datastruct.Decode(got["hll:hll0"])
	reg0, err := datastruct.DecodeHLL(ent.Payload)
	if err != nil {
		t.Fatalf("DecodeHLL hll0: %v", err)
	}
	if c := datastruct.HLLDenseCount(reg0); c < 1 || c > 10 {
		t.Fatalf("hll0 count=%d, want 1..10", c)
	}
	if len(st.Skipped) != 0 {
		t.Fatalf("skipped=%+v, want empty", st.Skipped)
	}
	// T1g 大 hash 可解且有序无关：200 field。
	ent, _ = datastruct.Decode(got["h:bigh"])
	hm, _, err = datastruct.DecodeHash(ent.Payload)
	if err != nil {
		t.Fatalf("DecodeHash big: %v", err)
	}
	if len(hm) != 200 || hm["f191"] != "v191" {
		t.Fatalf("bigh size=%d", len(hm))
	}
}

// T2 已过期跳过：ExpireMs=1（1970）的条目不落槽，计数。
func Test_Import_when_ExpiredSkipped(t *testing.T) {
	res := &Result{Entries: []Entry{
		{Key: "old", Type: TypeString, Str: []byte("x"), ExpireMs: 1},
		{Key: "new", Type: TypeString, Str: []byte("y")},
	}}
	mb := &memBatch{}
	st, err := Import(context.Background(), res, mb)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if st.Loaded != 1 || st.ExpiredSkipped != 1 || len(mb.ops) != 1 {
		t.Fatalf("stats=%+v ops=%d", st, len(mb.ops))
	}
	if string(mb.ops[0].Key) != "s:new" {
		t.Fatalf("key=%q", mb.ops[0].Key)
	}
}

// T3 list 保序：RDB 顺序即表顺序。
func Test_Import_when_ListOrder(t *testing.T) {
	res := loadFixture(t)
	mb := &memBatch{}
	if _, err := Import(context.Background(), res, mb); err != nil {
		t.Fatalf("Import: %v", err)
	}
	var raw []byte
	for _, op := range mb.ops {
		if string(op.Key) == "l:bigl" {
			raw = op.Value
		}
	}
	ent, err := datastruct.Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	elems, err := datastruct.DecodeList(ent.Payload)
	if err != nil {
		t.Fatalf("DecodeList: %v", err)
	}
	if len(elems) != 300 || elems[0] != "item-1" || elems[299] != "item-300" {
		t.Fatalf("list size=%d head=%q tail=%q", len(elems), elems[0], elems[len(elems)-1])
	}
	sort.Strings(elems)
	if elems[0] != "item-1" {
		t.Fatalf("sort check failed")
	}
}

// T4 dense HLL 往返：自组 HYLL blob（EncodeHLL）可导入且可校验。
func Test_Import_when_DenseHLL(t *testing.T) {
	reg := make([]byte, 12288)
	reg[0] = 0x3F
	reg[100] = 0xC0
	blob := datastruct.EncodeHLL(reg)
	res := &Result{Entries: []Entry{{Key: "h", Type: TypeHLL, HLL: blob}}}
	mb := &memBatch{}
	st, err := Import(context.Background(), res, mb)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if st.Loaded != 1 || len(mb.ops) != 1 {
		t.Fatalf("stats=%+v ops=%d", st, len(mb.ops))
	}
	if string(mb.ops[0].Key) != "hll:h" {
		t.Fatalf("key=%q", mb.ops[0].Key)
	}
	ent, err := datastruct.Decode(mb.ops[0].Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ent.Type != datastruct.TypeHLL {
		t.Fatalf("type=%q", []byte{ent.Type})
	}
	back, err := datastruct.DecodeHLL(ent.Payload)
	if err != nil {
		t.Fatalf("DecodeHLL: %v", err)
	}
	if back[0] != 0x3F || back[100] != 0xC0 {
		t.Fatalf("registers not preserved")
	}
}

// T5 stream 导入：fix-p1.rdb 的 stm1（4 条目 + g1 组 1 pending）经 Import 落槽，
// DecodeStream 还原 ID/字段/last/组/owner 关系。
func Test_Import_when_Stream(t *testing.T) {
	res, err := LoadFile("testdata/fix-p1.rdb")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	mb := &memBatch{}
	st, err := Import(context.Background(), res, mb)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	got := map[string][]byte{}
	for _, op := range mb.ops {
		got[string(op.Key)] = op.Value
	}
	raw, ok := got["x:stm1"]
	if !ok {
		t.Fatalf("stm1 not imported, stats=%+v", st)
	}
	ent, err := datastruct.Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ent.Type != datastruct.TypeStream {
		t.Fatalf("type=%q", []byte{ent.Type})
	}
	s, err := datastruct.DecodeStream(ent.Payload)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if len(s.Entries) != 4 {
		t.Fatalf("entries=%d, want 4", len(s.Entries))
	}
	e0 := s.Entries[0]
	if e0.ID.Ms != 1000 || e0.ID.Seq != 1 || len(e0.Fields) != 4 || e0.Fields[0] != "f1" || e0.Fields[1] != "v1" {
		t.Fatalf("e0=%+v", e0)
	}
	if !s.HasLast || s.Last.Ms != 1003 || s.Last.Seq != 1 {
		t.Fatalf("last=%+v haslast=%v", s.Last, s.HasLast)
	}
	if len(s.Groups) != 1 || s.Groups[0].Name != "g1" {
		t.Fatalf("groups=%+v", s.Groups)
	}
	g := s.Groups[0]
	if !g.HasRead || len(g.PEL) != 1 {
		t.Fatalf("group hasread=%v pel=%d", g.HasRead, len(g.PEL))
	}
	p := g.PEL[0]
	if p.ID.Ms != 1000 || p.ID.Seq != 1 || p.Consumer != "c1" || p.Count != 1 {
		t.Fatalf("pel=%+v", p)
	}
	if len(g.Consumers) != 1 || g.Consumers[0].Name != "c1" || !g.Consumers[0].HasActive {
		t.Fatalf("consumers=%+v", g.Consumers)
	}
}

// T6 P1 HLL 全形态：fix-p1 的 sparse（200）densify 后可数，dense（30226）直通。
func Test_Import_when_HLLBothForms(t *testing.T) {
	res, err := LoadFile("testdata/fix-p1.rdb")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	mb := &memBatch{}
	if _, err := Import(context.Background(), res, mb); err != nil {
		t.Fatalf("Import: %v", err)
	}
	got := map[string][]byte{}
	for _, op := range mb.ops {
		got[string(op.Key)] = op.Value
	}
	for key, want := range map[string][2]uint64{"hll:hllsparse": {150, 250}, "hll:hlldense": {29000, 31500}} {
		ent, err := datastruct.Decode(got[key])
		if err != nil {
			t.Fatalf("decode %s: %v", key, err)
		}
		reg, err := datastruct.DecodeHLL(ent.Payload)
		if err != nil {
			t.Fatalf("DecodeHLL %s: %v", key, err)
		}
		if c := datastruct.HLLDenseCount(reg); c < want[0] || c > want[1] {
			t.Fatalf("%s count=%d, want %d..%d", key, c, want[0], want[1])
		}
	}
}
