package datastruct

import (
	"reflect"
	"testing"
)

// Test_Vector_when_RoundTrip: 全精度向量 + 属性往返无损。
func Test_Vector_when_RoundTrip(t *testing.T) {
	// Given: 两个元素（一个带 JSON 属性，一个无）
	in := map[string]VEntry{
		"b": {Vec: []float64{1.5, -2.25, 3}, Attr: ""},
		"a": {Vec: []float64{0, 1, -0.5}, Attr: `{"color":"red"}`},
	}
	// When: 编码再解码
	raw, err := EncodeVector(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeVector(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Then: 完全一致
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("roundtrip mismatch:\n got=%v\nwant=%v", got, in)
	}
}

// Test_Vector_when_Empty: 空表合法。
func Test_Vector_when_Empty(t *testing.T) {
	raw, err := EncodeVector(map[string]VEntry{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeVector(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %v", got)
	}
}

// Test_Vector_when_DimMismatch: 维度不一致拒绝。
func Test_Vector_when_DimMismatch(t *testing.T) {
	in := map[string]VEntry{
		"a": {Vec: []float64{1, 2}},
		"b": {Vec: []float64{1, 2, 3}},
	}
	if _, err := EncodeVector(in); err == nil {
		t.Fatal("want dim mismatch error, got nil")
	}
}

// Test_Vector_when_Truncated: 截断输入逐段报错。
func Test_Vector_when_Truncated(t *testing.T) {
	raw, err := EncodeVector(map[string]VEntry{"a": {Vec: []float64{1}}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for i := 0; i < len(raw); i++ {
		if _, err := DecodeVector(raw[:i]); err == nil {
			t.Fatalf("prefix len %d: want error, got nil", i)
		}
	}
}
