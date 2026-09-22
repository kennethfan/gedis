package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Given: HELLO 命令（无版本参数）
// When: ParseHello
// Then: 默认版本 3
func Test_ParseHello_when_NoVersion(t *testing.T) {
	ver, err := ParseHello(ArrayOf(BulkOf("HELLO")))
	require.NoError(t, err)
	require.Equal(t, 3, ver)
}

// Given: HELLO 2 / hello 3（大小写不敏感，bulk 或 integer 参数）
// When: ParseHello
// Then: 返回对应版本
func Test_ParseHello_when_WithVersion(t *testing.T) {
	ver, err := ParseHello(ArrayOf(BulkOf("HELLO"), BulkOf("2")))
	require.NoError(t, err)
	require.Equal(t, 2, ver)

	ver, err = ParseHello(ArrayOf(BulkOf("hello"), Value{Kind: KindInteger, I: 3}))
	require.NoError(t, err)
	require.Equal(t, 3, ver)
}

// Given: 非 HELLO 命令 / 非法版本
// When: ParseHello
// Then: 返回错误
func Test_ParseHello_when_Invalid(t *testing.T) {
	_, err := ParseHello(ArrayOf(BulkOf("GET"), BulkOf("k")))
	require.Error(t, err)

	_, err = ParseHello(ArrayOf(BulkOf("HELLO"), BulkOf("9")))
	require.Error(t, err)

	_, err = ParseHello(BulkOf("HELLO"))
	require.Error(t, err)
}
