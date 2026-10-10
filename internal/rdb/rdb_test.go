package rdb

import (
	"math"
	"testing"
)

func loadP0(t *testing.T) *Result {
	t.Helper()
	res, err := LoadFile("testdata/fix-p0.rdb")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	return res
}

func byKey(res *Result) map[string]Entry {
	m := map[string]Entry{}
	for _, e := range res.Entries {
		m[e.Key] = e
	}
	return m
}

func Test_Load_P0_KeySet(t *testing.T) {
	res := loadP0(t)
	if len(res.Skipped) != 0 {
		t.Fatalf("skipped = %v, want none", res.Skipped)
	}
	m := byKey(res)
	wantType := map[string]ValueType{
		"str0": "string", "strexp": "string",
		"h0": "hash", "bigh": "hash",
		"l0": "list", "bigl": "list",
		"s0": "set", "intset0": "set",
		"z0": "zset", "bigz": "zset",
		"hll0": "hll",
	}
	if len(m) != len(wantType) {
		t.Fatalf("keys = %d, want %d", len(m), len(wantType))
	}
	for k, wt := range wantType {
		e, ok := m[k]
		if !ok {
			t.Fatalf("missing key %q", k)
		}
		if e.Type != wt {
			t.Fatalf("key %q type = %s, want %s", k, e.Type, wt)
		}
	}
}

func Test_Load_P0_Values(t *testing.T) {
	m := byKey(loadP0(t))
	if string(m["str0"].Str) != "hello" {
		t.Fatalf("str0 = %q", m["str0"].Str)
	}
	if string(m["strexp"].Str) != "world" {
		t.Fatalf("strexp = %q", m["strexp"].Str)
	}
	if m["h0"].Hash["f1"] != "v1" || m["h0"].Hash["f2"] != "v2" || len(m["h0"].Hash) != 2 {
		t.Fatalf("h0 = %v", m["h0"].Hash)
	}
	if len(m["bigh"].Hash) != 200 || m["bigh"].Hash["f200"] != "v200" {
		t.Fatalf("bigh len = %d", len(m["bigh"].Hash))
	}
	if len(m["l0"].List) != 3 || m["l0"].List[0] != "a" || m["l0"].List[2] != "c" {
		t.Fatalf("l0 = %v", m["l0"].List)
	}
	if len(m["bigl"].List) != 300 || m["bigl"].List[0] != "item-1" || m["bigl"].List[299] != "item-300" {
		t.Fatalf("bigl len = %d first/last = %q/%q", len(m["bigl"].List), m["bigl"].List[0], m["bigl"].List[299])
	}
	setOf := func(es []string) map[string]bool {
		s := map[string]bool{}
		for _, x := range es {
			s[x] = true
		}
		return s
	}
	s0 := setOf(m["s0"].Set)
	if len(s0) != 3 || !s0["m1"] || !s0["m2"] || !s0["m3"] {
		t.Fatalf("s0 = %v", m["s0"].Set)
	}
	is0 := setOf(m["intset0"].Set)
	if len(is0) != 50 || !is0["1"] || !is0["50"] {
		t.Fatalf("intset0 len = %d", len(is0))
	}
	z0 := map[string]float64{}
	for _, ms := range m["z0"].ZSet {
		z0[ms.Member] = ms.Score
	}
	if len(z0) != 2 || z0["a"] != 1.5 || z0["b"] != 2.5 {
		t.Fatalf("z0 = %v", m["z0"].ZSet)
	}
	if len(m["bigz"].ZSet) != 200 {
		t.Fatalf("bigz len = %d", len(m["bigz"].ZSet))
	}
	found := false
	for _, ms := range m["bigz"].ZSet {
		if ms.Member == "m200" {
			found = true
			if ms.Score != 200 {
				t.Fatalf("bigz m200 score = %v", ms.Score)
			}
		}
	}
	if !found {
		t.Fatalf("bigz missing m200")
	}
	if !isHYLL(m["hll0"].HLL) {
		t.Fatalf("hll0 not HYLL blob, len=%d", len(m["hll0"].HLL))
	}
}

func Test_Load_P0_Expiry(t *testing.T) {
	m := byKey(loadP0(t))
	if m["strexp"].ExpireMs <= 0 {
		t.Fatalf("strexp expire = %d, want >0", m["strexp"].ExpireMs)
	}
	if m["h0"].ExpireMs <= 0 {
		t.Fatalf("h0 expire = %d, want >0", m["h0"].ExpireMs)
	}
	if m["str0"].ExpireMs != 0 {
		t.Fatalf("str0 expire = %d, want 0", m["str0"].ExpireMs)
	}
}

func Test_Load_BadVersion(t *testing.T) {
	data := append([]byte("REDIS0009"), make([]byte, 16)...)
	res, err := Load(bytesReader(data))
	if err == nil || res != nil {
		t.Fatalf("want version error, got %v", err)
	}
}

func Test_Load_BadChecksum(t *testing.T) {
	raw, err := osRead("testdata/fix-p0.rdb")
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if _, err := Load(bytesReader(raw)); err == nil {
		t.Fatalf("want checksum error")
	}
}

func Test_Load_StreamPending(t *testing.T) {
	// 最小合成流：仅验证遇到 stream 类型即 Loud 失败（结构解析待 P1）。
	if _, err := Load(bytesReader(minimalStreamRDB())); err == nil {
		t.Fatalf("want stream pending error")
	}
}

func Test_LZF_Roundtrip(t *testing.T) {
	// 手工 LZF 流：字面 "abc" + 回指 "abc"。
	comp := []byte{0x02, 'a', 'b', 'c', 0x20, 0x02}
	r := lzfTestReader(comp, len(comp), 6)
	got, err := readLZF(r)
	if err != nil {
		t.Fatalf("readLZF: %v", err)
	}
	if string(got) != "abcabc" {
		t.Fatalf("lzf = %q", got)
	}
}

func Test_CRC64_Vector(t *testing.T) {
	// "123456789" 向量（与真机 RDB checksum 同算法）。
	if got := crc64(0, []byte("123456789")); got != 0xe9c6d914c4b8d9ca {
		t.Fatalf("crc64 = %#x", got)
	}
}

func Test_ZSet2_BinaryDouble(t *testing.T) {
	bits := math.Float64bits(3.25)
	var buf [8]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(bits >> (8 * i))
	}
	r := lzfTestReader(buf[:], 0, 0)
	_ = r
	rr := newReader(bytesReader(buf[:]))
	f, err := readDoubleBin(rr)
	if err != nil {
		t.Fatalf("readDoubleBin: %v", err)
	}
	if f != 3.25 {
		t.Fatalf("double = %v", f)
	}
}
