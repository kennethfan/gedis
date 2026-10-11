package rdb

import (
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/kennethfan/gedis/internal/datastruct"
)

// roundtrip 导出→Load 解析→逐条比对（stream/HLL 归一化哨兵后比）。
func Test_Export_when_Roundtrip(t *testing.T) {
	hllDense := datastruct.EncodeHLL(nil)
	in := []Entry{
		{Key: "s", Type: TypeString, Str: []byte("hello")},
		{Key: "e", Type: TypeString, Str: []byte("x"), ExpireMs: 1893456000000},
		{Key: "l", Type: TypeList, List: []string{"c", "a", "b"}},
		{Key: "t", Type: TypeSet, Set: []string{"m2", "m1"}},
		{Key: "z", Type: TypeZSet, ZSet: []MemberScore{{"b", 2.5}, {"a", math.Inf(1)}, {"n", math.NaN()}}},
		{Key: "h", Type: TypeHash, Hash: map[string]string{"f2": "v2", "f1": "v1"}},
		{Key: "p", Type: TypeHLL, HLL: hllDense},
		{Key: "x", Type: TypeStream, Stream: &StreamData{
			Entries: []StreamEntry{
				{MS: 1000, Seq: 1, Fields: [][2]string{{"f", "v"}, {"a", "b"}}},
				{MS: 1001, Seq: 2, Fields: [][2]string{{"f", "w"}}},
				{MS: 900, Seq: 9, Fields: [][2]string{{"k", "z"}}},
			},
			Groups: []StreamGroupData{{
				Name: "g1", LastMS: 1001, LastSeq: 2, EntriesRead: 1, HasEntriesRead: true,
				Pending:   []StreamPending{{MS: 1000, Seq: 1, DeliveryTime: 123, DeliveryCount: 2, Consumer: "c1"}},
				Consumers: []StreamConsumerData{{Name: "c1", SeenTime: 456}},
			}},
		}},
	}
	var buf bytes.Buffer
	if err := Export(&buf, in); err != nil {
		t.Fatalf("export: %v", err)
	}
	got, err := Load(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("load own export: %v", err)
	}
	if len(got.Entries) != len(in) {
		t.Fatalf("entries %d != %d", len(got.Entries), len(in))
	}
	for i, want := range in {
		g := got.Entries[i]
		if g.Key != want.Key || g.Type != want.Type || g.ExpireMs != want.ExpireMs {
			t.Fatalf("entry %d meta %+v != %+v", i, g, want)
		}
		switch want.Type {
		case TypeString:
			if !bytes.Equal(g.Str, want.Str) {
				t.Fatalf("%s str %q != %q", want.Key, g.Str, want.Str)
			}
		case TypeList:
			if !reflect.DeepEqual(g.List, want.List) {
				t.Fatalf("list %+v != %+v", g.List, want.List)
			}
		case TypeSet:
			if !reflect.DeepEqual(g.Set, []string{"m1", "m2"}) {
				t.Fatalf("set %+v", g.List)
			}
		case TypeZSet:
			if len(g.ZSet) != 3 || g.ZSet[0].Member != "a" || g.ZSet[1].Member != "b" || g.ZSet[2].Member != "n" {
				t.Fatalf("zset %+v", g.ZSet)
			}
			if g.ZSet[1].Score != 2.5 || !math.IsInf(g.ZSet[0].Score, 1) || !math.IsNaN(g.ZSet[2].Score) {
				t.Fatalf("zset scores %+v", g.ZSet)
			}
		case TypeHash:
			if !reflect.DeepEqual(g.Hash, want.Hash) {
				t.Fatalf("hash %+v", g.Hash)
			}
		case TypeHLL:
			if !bytes.Equal(g.HLL, want.HLL) {
				t.Fatalf("hll len %d", len(g.HLL))
			}
		case TypeStream:
			assertStreamRoundtrip(t, g.Stream, want.Stream)
		}
	}
}

func assertStreamRoundtrip(t *testing.T, g, want *StreamData) {
	t.Helper()
	if !reflect.DeepEqual(g.Entries, want.Entries) {
		t.Fatalf("stream entries %+v != %+v", g.Entries, want.Entries)
	}
	if g.LastMS != 900 || g.LastSeq != 9 {
		t.Fatalf("stream last %d-%d", g.LastMS, g.LastSeq)
	}
	if len(g.Groups) != 1 {
		t.Fatalf("groups %d", len(g.Groups))
	}
	gg := g.Groups[0]
	if gg.Name != "g1" || !gg.HasEntriesRead || gg.EntriesRead != 1 {
		t.Fatalf("group %+v", gg)
	}
	if len(gg.Pending) != 1 || gg.Pending[0].Consumer != "c1" || gg.Pending[0].DeliveryCount != 2 {
		t.Fatalf("pending %+v", gg.Pending)
	}
	if len(gg.Consumers) != 1 || gg.Consumers[0].SeenTime != 456 {
		t.Fatalf("consumers %+v", gg.Consumers)
	}
	// ActiveTime 0→导出 -1 哨兵→回读 MaxUint64，归一化不断言。
}
