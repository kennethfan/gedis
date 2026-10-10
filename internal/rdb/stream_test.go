package rdb

import (
	"testing"
)

// P1：fix-p1.rdb（7.2.6）stm1：4 条 XADD + g1/c1 一条 pending。
func Test_Stream_when_DecodeStm1(t *testing.T) {
	res, err := LoadFile("testdata/fix-p1.rdb")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	var got *Entry
	for i := range res.Entries {
		if res.Entries[i].Key == "stm1" {
			got = &res.Entries[i]
		}
	}
	if got == nil {
		t.Fatalf("stm1 not found")
	}
	if got.Type != TypeStream || got.Stream == nil {
		t.Fatalf("type=%q stream=nil", got.Type)
	}
	st := got.Stream
	wantEntries := []StreamEntry{
		{MS: 1000, Seq: 1, Fields: [][2]string{{"f1", "v1"}, {"f2", "v2"}}},
		{MS: 1001, Seq: 1, Fields: [][2]string{{"a", "b"}}},
		{MS: 1002, Seq: 5, Fields: [][2]string{{"k", "v"}}},
		{MS: 1003, Seq: 1, Fields: [][2]string{{"z", "z"}}},
	}
	if len(st.Entries) != len(wantEntries) {
		t.Fatalf("entries=%d want %d", len(st.Entries), len(wantEntries))
	}
	for i, w := range wantEntries {
		e := st.Entries[i]
		if e.MS != w.MS || e.Seq != w.Seq {
			t.Fatalf("entry %d id=%d-%d want %d-%d", i, e.MS, e.Seq, w.MS, w.Seq)
		}
		if len(e.Fields) != len(w.Fields) {
			t.Fatalf("entry %d fields=%v want %v", i, e.Fields, w.Fields)
		}
		for j, f := range w.Fields {
			if e.Fields[j] != f {
				t.Fatalf("entry %d field %d=%v want %v", i, j, e.Fields[j], f)
			}
		}
	}
	if st.LastMS != 1003 || st.LastSeq != 1 {
		t.Fatalf("last=%d-%d", st.LastMS, st.LastSeq)
	}
	if st.FirstMS != 1000 || st.FirstSeq != 1 {
		t.Fatalf("first=%d-%d", st.FirstMS, st.FirstSeq)
	}
	if st.Added != 4 {
		t.Fatalf("added=%d", st.Added)
	}
	if st.MaxDeletedMS != 0 || st.MaxDeletedSeq != 0 {
		t.Fatalf("maxdeleted=%d-%d", st.MaxDeletedMS, st.MaxDeletedSeq)
	}
	if len(st.Groups) != 1 {
		t.Fatalf("groups=%d", len(st.Groups))
	}
	g := st.Groups[0]
	if g.Name != "g1" {
		t.Fatalf("group=%q", g.Name)
	}
	if g.LastMS != 1000 || g.LastSeq != 1 {
		t.Fatalf("group last=%d-%d", g.LastMS, g.LastSeq)
	}
	if len(g.Pending) != 1 {
		t.Fatalf("pending=%d", len(g.Pending))
	}
	p := g.Pending[0]
	if p.MS != 1000 || p.Seq != 1 || p.Consumer != "c1" || p.DeliveryCount != 1 {
		t.Fatalf("pending=%+v", p)
	}
	if p.DeliveryTime == 0 {
		t.Fatalf("delivery time not set")
	}
	if len(g.Consumers) != 1 || g.Consumers[0].Name != "c1" {
		t.Fatalf("consumers=%+v", g.Consumers)
	}
	if g.Consumers[0].SeenTime == 0 || g.Consumers[0].ActiveTime == 0 {
		t.Fatalf("consumer times not set: %+v", g.Consumers[0])
	}
}

// P1：HLL sparse/dense 均以 HYLL blob 呈现（STRLEN 512 / 12304）。
func Test_HLL_when_P1Blobs(t *testing.T) {
	res, err := LoadFile("testdata/fix-p1.rdb")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	byKey := map[string]*Entry{}
	for i := range res.Entries {
		byKey[res.Entries[i].Key] = &res.Entries[i]
	}
	sp, ok := byKey["hllsparse"]
	if !ok || sp.Type != TypeHLL || len(sp.HLL) != 512 {
		t.Fatalf("hllsparse: %+v", sp)
	}
	de, ok := byKey["hlldense"]
	if !ok || de.Type != TypeHLL || len(de.HLL) != 12304 {
		t.Fatalf("hlldense: %+v", de)
	}
}
