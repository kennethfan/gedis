package datastruct

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Given: payload + 无过期
// When: EncodeString 后 Decode
// Then: 类型为 TypeString，Expiry 为 0，payload 还原
func Test_Codec_when_RoundTripNoExpiry(t *testing.T) {
	raw := EncodeString([]byte("hello"), 0)
	e, err := Decode(raw)
	require.NoError(t, err)
	require.Equal(t, TypeString, e.Type)
	require.Zero(t, e.Expiry)
	require.Equal(t, []byte("hello"), e.Payload)
}

// Given: payload + 过期时间戳
// When: EncodeString 后 Decode
// Then: Expiry 还原
func Test_Codec_when_RoundTripWithExpiry(t *testing.T) {
	raw := EncodeString([]byte("v"), 1700000000000000000)
	e, err := Decode(raw)
	require.NoError(t, err)
	require.Equal(t, int64(1700000000000000000), e.Expiry)
	require.Equal(t, []byte("v"), e.Payload)
}

// Given: 截断/空字节串
// When: Decode
// Then: 返回错误
func Test_Codec_when_Truncated(t *testing.T) {
	_, err := Decode(nil)
	require.Error(t, err)

	_, err = Decode([]byte{TypeString, 0, 1})
	require.Error(t, err)
}

// Given: 非 string 类型标签
// When: Decode
// Then: Type 原样返回（WRONGTYPE 由调用方判定）
func Test_Codec_when_OtherType(t *testing.T) {
	raw := Encode(TypeHash, 0, []byte("f"))
	e, err := Decode(raw)
	require.NoError(t, err)
	require.Equal(t, TypeHash, e.Type)
}

// Given: 用户 key
// When: StringKey
// Then: 加 s: 前缀
func Test_StringKey_when_Prefix(t *testing.T) {
	require.Equal(t, []byte("s:mykey"), StringKey("mykey"))
}
