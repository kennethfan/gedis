package protocol

import (
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Given: "+OK\r\n"
// When: Decode
// Then: KindSimpleString，内容为 OK
func Test_Decode_when_SimpleString(t *testing.T) {
	v, err := Decode(bufio.NewReader(strings.NewReader("+OK\r\n")))
	require.NoError(t, err)
	require.Equal(t, KindSimpleString, v.Kind)
	require.Equal(t, "OK", v.S)
}

// Given: "-ERR bad\r\n"
// When: Decode
// Then: KindError，内容为 ERR bad
func Test_Decode_when_Error(t *testing.T) {
	v, err := Decode(bufio.NewReader(strings.NewReader("-ERR bad\r\n")))
	require.NoError(t, err)
	require.Equal(t, KindError, v.Kind)
	require.Equal(t, "ERR bad", v.S)
}

// Given: ":42\r\n"
// When: Decode
// Then: KindInteger，值为 42
func Test_Decode_when_Integer(t *testing.T) {
	v, err := Decode(bufio.NewReader(strings.NewReader(":42\r\n")))
	require.NoError(t, err)
	require.Equal(t, KindInteger, v.Kind)
	require.Equal(t, int64(42), v.I)
}

// Given: "$5\r\nhello\r\n"
// When: Decode
// Then: KindBulkString，内容为 hello
func Test_Decode_when_BulkString(t *testing.T) {
	v, err := Decode(bufio.NewReader(strings.NewReader("$5\r\nhello\r\n")))
	require.NoError(t, err)
	require.Equal(t, KindBulkString, v.Kind)
	require.Equal(t, []byte("hello"), v.Bulk)
}

// Given: "$-1\r\n"
// When: Decode
// Then: KindBulkString 且 Bulk 为 nil（null）
func Test_Decode_when_NullBulkString(t *testing.T) {
	v, err := Decode(bufio.NewReader(strings.NewReader("$-1\r\n")))
	require.NoError(t, err)
	require.Equal(t, KindBulkString, v.Kind)
	require.Nil(t, v.Bulk)
}

// Given: "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
// When: Decode
// Then: 2 元 array，元素为 bulk GET / key
func Test_Decode_when_Array(t *testing.T) {
	v, err := Decode(bufio.NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")))
	require.NoError(t, err)
	require.Equal(t, KindArray, v.Kind)
	require.Len(t, v.Elems, 2)
	require.Equal(t, []byte("GET"), v.Elems[0].Bulk)
	require.Equal(t, []byte("key"), v.Elems[1].Bulk)
}

// Given: "*-1\r\n"
// When: Decode
// Then: KindArray 且 Elems 为 nil（null）
func Test_Decode_when_NullArray(t *testing.T) {
	v, err := Decode(bufio.NewReader(strings.NewReader("*-1\r\n")))
	require.NoError(t, err)
	require.Equal(t, KindArray, v.Kind)
	require.Nil(t, v.Elems)
}

// Given: "PING\r\n"（inline 命令）
// When: Decode
// Then: 1 元 array，元素为 bulk PING
func Test_Decode_when_InlineCommand(t *testing.T) {
	v, err := Decode(bufio.NewReader(strings.NewReader("PING\r\n")))
	require.NoError(t, err)
	require.Equal(t, KindArray, v.Kind)
	require.Len(t, v.Elems, 1)
	require.Equal(t, []byte("PING"), v.Elems[0].Bulk)
}

// Given: "SET key value\r\n"（多词 inline）
// When: Decode
// Then: 3 元 array，按空格切分
func Test_Decode_when_InlineMultWord(t *testing.T) {
	v, err := Decode(bufio.NewReader(strings.NewReader("SET key value\r\n")))
	require.NoError(t, err)
	require.Equal(t, KindArray, v.Kind)
	require.Len(t, v.Elems, 3)
}
