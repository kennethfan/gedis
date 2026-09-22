package protocol

import (
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Given: 各种畸形报文
// When: Decode
// Then: 返回错误（覆盖错误分支）
func Test_Decode_when_Malformed(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"no CRLF", "+OK"},
		{"bad int", ":abc\r\n"},
		{"bad array len", "*x\r\n"},
		{"negative bulk", "$-2\r\n"},
		{"truncated bulk", "$5\r\nhi\r\n"},
		{"bulk no CRLF", "$2\r\nhixx"},
		{"bad map len", "%x\r\n"},
		{"map odd", "%1\r\n+a\r\n"},
		{"bad set len", "~-1\r\n"},
		{"bad push", ">-1\r\n"},
		{"bad bool", "#x\r\n"},
		{"bool truncated", "#"},
		{"bad double", ",nanx\r\n"},
		{"verbatim no fmt", "=5\r\nhello\r\n"},
		{"verbatim bad len", "=x\r\n"},
		{"sized neg", "!-1\r\n"},
		{"null no CRLF", "_xx"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Decode(bufio.NewReader(strings.NewReader(c.raw)))
			require.Error(t, err)
		})
	}
}

// Given: 非法 HELLO 值
// When: ParseHello
// Then: 返回错误
func Test_ParseHello_when_ErrorBranches(t *testing.T) {
	_, err := ParseHello(ArrayOf(BulkOf("HELLO"), BulkOf("x")))
	require.Error(t, err)

	_, err = ParseHello(ArrayOf(BulkOf("HELLO"), Value{Kind: KindInteger, I: 9}))
	require.Error(t, err)

	_, err = ParseHello(ArrayOf(BulkOf("HELLO"), Value{Kind: KindNull}))
	require.Error(t, err)

	_, err = ParseHello(Value{Kind: KindBulkString, Bulk: []byte("HELLO")})
	require.Error(t, err)
}

// Given: 未知 Kind
// When: Append
// Then: 编码为 error（default 分支）
func Test_Append_when_UnknownKind(t *testing.T) {
	v := Value{Kind: Kind(99)}
	require.Equal(t, "-ERR unknown kind\r\n", string(v.Append(nil)))
}
