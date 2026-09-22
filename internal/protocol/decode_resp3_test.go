package protocol

import (
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func decodeOne(t *testing.T, raw string) Value {
	t.Helper()
	v, err := Decode(bufio.NewReader(strings.NewReader(raw)))
	require.NoError(t, err)
	return v
}

// Given: "_\r\n"
// When: Decode
// Then: KindNull
func Test_Decode_when_Null(t *testing.T) {
	require.Equal(t, KindNull, decodeOne(t, "_\r\n").Kind)
}

// Given: "#t\r\n" / "#f\r\n"
// When: Decode
// Then: KindBoolean，真/假
func Test_Decode_when_Boolean(t *testing.T) {
	require.True(t, decodeOne(t, "#t\r\n").B)
	require.False(t, decodeOne(t, "#f\r\n").B)
	require.Equal(t, KindBoolean, decodeOne(t, "#t\r\n").Kind)
}

// Given: ",1.23\r\n"
// When: Decode
// Then: KindDouble，值为 1.23
func Test_Decode_when_Double(t *testing.T) {
	v := decodeOne(t, ",1.23\r\n")
	require.Equal(t, KindDouble, v.Kind)
	require.InEpsilon(t, 1.23, v.F, 1e-9)
}

// Given: "(3492890328409238509324850943850943825024385\r\n"
// When: Decode
// Then: KindBigNumber，原文保留在 S
func Test_Decode_when_BigNumber(t *testing.T) {
	v := decodeOne(t, "(3492890328409238509324850943850943825024385\r\n")
	require.Equal(t, KindBigNumber, v.Kind)
	require.Equal(t, "3492890328409238509324850943850943825024385", v.S)
}

// Given: "!21\r\nSYNTAX invalid syntax\r\n"
// When: Decode
// Then: KindBulkError，内容保留
func Test_Decode_when_BulkError(t *testing.T) {
	v := decodeOne(t, "!21\r\nSYNTAX invalid syntax\r\n")
	require.Equal(t, KindBulkError, v.Kind)
	require.Equal(t, []byte("SYNTAX invalid syntax"), v.Bulk)
}

// Given: "=15\r\ntxt:Some string\r\n"
// When: Decode
// Then: KindVerbatim，格式 txt，内容 Some string
func Test_Decode_when_Verbatim(t *testing.T) {
	v := decodeOne(t, "=15\r\ntxt:Some string\r\n")
	require.Equal(t, KindVerbatim, v.Kind)
	require.Equal(t, "txt", v.VerbatimFmt)
	require.Equal(t, []byte("Some string"), v.Bulk)
}

// Given: "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n"
// When: Decode
// Then: KindMap，2 对保序 Pairs
func Test_Decode_when_Map(t *testing.T) {
	v := decodeOne(t, "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n")
	require.Equal(t, KindMap, v.Kind)
	require.Len(t, v.Pairs, 2)
	require.Equal(t, "first", v.Pairs[0].K.S)
	require.Equal(t, int64(1), v.Pairs[0].V.I)
	require.Equal(t, "second", v.Pairs[1].K.S)
}

// Given: "~3\r\n+orange\r\n+apple\r\n#t\r\n"
// When: Decode
// Then: KindSet，3 个元素
func Test_Decode_when_Set(t *testing.T) {
	v := decodeOne(t, "~3\r\n+orange\r\n+apple\r\n#t\r\n")
	require.Equal(t, KindSet, v.Kind)
	require.Len(t, v.Elems, 3)
}

// Given: "|1\r\n+key-popularity\r\n%2\r\n$a\r\n,b\r\n"
// When: Decode（attribute 包着 map）
// Then: KindAttribute，Pairs 保留
func Test_Decode_when_Attribute(t *testing.T) {
	v := decodeOne(t, "|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,1\r\n$1\r\nb\r\n,2\r\n")
	require.Equal(t, KindAttribute, v.Kind)
	require.Len(t, v.Pairs, 1)
	require.Equal(t, "key-popularity", v.Pairs[0].K.S)
	require.Equal(t, KindMap, v.Pairs[0].V.Kind)
}

// Given: ">4\r\n+message\r\n+somechannel\r\n+this is the message\r\n"
// When: Decode
// Then: KindPush
func Test_Decode_when_Push(t *testing.T) {
	v := decodeOne(t, ">3\r\n+message\r\n+somechannel\r\n+this is the message\r\n")
	require.Equal(t, KindPush, v.Kind)
	require.Len(t, v.Elems, 3)
}

// Given: "$?\r\n;\r\n"（流式 chunk，非本版支持）
// When: Decode
// Then: 返回错误而非 panic
func Test_Decode_when_StreamingChunk(t *testing.T) {
	_, err := Decode(bufio.NewReader(strings.NewReader("$?\r\n")))
	require.Error(t, err)
}
