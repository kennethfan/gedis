package protocol

import (
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Given: 各类型 Value
// When: Append
// Then: 输出标准 RESP 字节
func Test_Append_when_AllKinds(t *testing.T) {
	cases := []struct {
		name string
		v    Value
		want string
	}{
		{"simple", Value{Kind: KindSimpleString, S: "OK"}, "+OK\r\n"},
		{"error", Value{Kind: KindError, S: "ERR bad"}, "-ERR bad\r\n"},
		{"int", Value{Kind: KindInteger, I: -42}, ":-42\r\n"},
		{"bulk", BulkOf("hello"), "$5\r\nhello\r\n"},
		{"nullbulk", Value{Kind: KindBulkString}, "$-1\r\n"},
		{"array", ArrayOf(BulkOf("GET"), BulkOf("k")), "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"},
		{"nullarray", Value{Kind: KindArray}, "*-1\r\n"},
		{"emptyarray", Value{Kind: KindArray, Elems: []Value{}}, "*0\r\n"},
		{"null", Value{Kind: KindNull}, "_\r\n"},
		{"true", Value{Kind: KindBoolean, B: true}, "#t\r\n"},
		{"false", Value{Kind: KindBoolean}, "#f\r\n"},
		{"double", Value{Kind: KindDouble, F: 1.5}, ",1.5\r\n"},
		{"bignum", Value{Kind: KindBigNumber, S: "349"}, "(349\r\n"},
		{"bulkerr", Value{Kind: KindBulkError, Bulk: []byte("SYNTAX")}, "!6\r\nSYNTAX\r\n"},
		{"verbatim", Value{Kind: KindVerbatim, VerbatimFmt: "txt", Bulk: []byte("hi")}, "=6\r\ntxt:hi\r\n"},
		{
			"map",
			Value{Kind: KindMap, Pairs: []Pair{{K: BulkOf("a"), V: Value{Kind: KindInteger, I: 1}}}},
			"%1\r\n$1\r\na\r\n:1\r\n",
		},
		{"set", Value{Kind: KindSet, Elems: []Value{BulkOf("x")}}, "~1\r\n$1\r\nx\r\n"},
		{"push", Value{Kind: KindPush, Elems: []Value{BulkOf("m")}}, ">1\r\n$1\r\nm\r\n"},
		{
			"attr",
			Value{Kind: KindAttribute, Pairs: []Pair{{K: BulkOf("k"), V: BulkOf("v")}}},
			"|1\r\n$1\r\nk\r\n$1\r\nv\r\n",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, string(c.v.Append(nil)))
		})
	}
}

// Given: 一段 RESP3 报文
// When: Decode 后 Append
// Then: 得到原文（roundtrip 稳定）
func Test_RoundTrip_when_DecodeThenAppend(t *testing.T) {
	raws := []string{
		"+OK\r\n",
		":42\r\n",
		"$5\r\nhello\r\n",
		"$-1\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
		"_\r\n",
		"#t\r\n",
		",1.23\r\n",
		"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n",
		"~2\r\n+a\r\n+b\r\n",
		"=12\r\ntxt:hello wo\r\n",
	}
	for _, raw := range raws {
		v, err := Decode(bufio.NewReader(strings.NewReader(raw)))
		require.NoError(t, err, raw)
		require.Equal(t, raw, string(v.Append(nil)), raw)
	}
}
