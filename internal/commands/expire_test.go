package commands

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Given: 已存在的无过期 key
// When: EXPIRE k 100
// Then: 返回 1，TTL 约为 100
func Test_Expire_when_SetTTL(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v")
	require.Equal(t, int64(1), dispatch(r, "EXPIRE", "k", "100").I)
	ttl := dispatch(r, "TTL", "k").I
	require.GreaterOrEqual(t, ttl, int64(99))
	require.LessOrEqual(t, ttl, int64(100))
}

// Given: 不存在的 key
// When: EXPIRE / PEXPIRE / PERSIST / TTL / PTTL
// Then: EXPIRE 系返回 0，TTL 系返回 -2
func Test_Expire_when_Missing(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, int64(0), dispatch(r, "EXPIRE", "nope", "100").I)
	require.Equal(t, int64(0), dispatch(r, "PEXPIRE", "nope", "100000").I)
	require.Equal(t, int64(0), dispatch(r, "PERSIST", "nope").I)
	require.Equal(t, int64(-2), dispatch(r, "TTL", "nope").I)
	require.Equal(t, int64(-2), dispatch(r, "PTTL", "nope").I)
}

// Given: 已存在的 key
// When: EXPIRE k 0（非正超时）
// Then: 返回 1 且 key 被删除
func Test_Expire_when_NonPositiveDeletes(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v")
	require.Equal(t, int64(1), dispatch(r, "EXPIRE", "k", "0").I)
	require.Equal(t, int64(0), dispatch(r, "EXISTS", "k").I)
}

// Given: 已存在的无过期 key
// When: PEXPIRE k 100000
// Then: 返回 1，PTTL 落在 (99000, 100000] 区间
func Test_Expire_when_PExpire(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v")
	require.Equal(t, int64(1), dispatch(r, "PEXPIRE", "k", "100000").I)
	pttl := dispatch(r, "PTTL", "k").I
	require.Greater(t, pttl, int64(99000))
	require.LessOrEqual(t, pttl, int64(100000))
}

// Given: 带过期的 key
// When: PERSIST k
// Then: 返回 1，TTL 变为 -1；再次 PERSIST 返回 0
func Test_Expire_when_Persist(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v", "EX", "100")
	require.Equal(t, int64(1), dispatch(r, "PERSIST", "k").I)
	require.Equal(t, int64(-1), dispatch(r, "TTL", "k").I)
	require.Equal(t, int64(0), dispatch(r, "PERSIST", "k").I)
}

// Given: 已存在的 key
// When: EXPIREAT k <过去时间戳>
// Then: 返回 1 且 key 被删除（被动过期语义）
func Test_Expire_when_ExpireAtPastDeletes(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v")
	past := time.Now().Add(-time.Hour).Unix()
	require.Equal(t, int64(1), dispatch(r, "EXPIREAT", "k", itoa(past)).I)
	require.Equal(t, int64(0), dispatch(r, "EXISTS", "k").I)
}

// Given: 已存在的 key
// When: PEXPIREAT k <未来毫秒时间戳>
// Then: 返回 1，PTTL 大于 0
func Test_Expire_when_PExpireAtFuture(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v")
	future := time.Now().Add(time.Hour).UnixMilli()
	require.Equal(t, int64(1), dispatch(r, "PEXPIREAT", "k", itoa(future)).I)
	require.Greater(t, dispatch(r, "PTTL", "k").I, int64(0))
}

// Given: 无过期 key 与有过期 key
// When: 带 NX / XX 选项的 EXPIRE
// Then: NX 仅无过期时设置，XX 仅有过期时设置
func Test_Expire_when_NxXx(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "a", "v")
	dispatch(r, "SET", "b", "v", "EX", "100")
	require.Equal(t, int64(1), dispatch(r, "EXPIRE", "a", "100", "NX").I)
	require.Equal(t, int64(0), dispatch(r, "EXPIRE", "a", "200", "NX").I)
	require.Equal(t, int64(1), dispatch(r, "EXPIRE", "a", "200", "XX").I)
	require.Equal(t, int64(0), dispatch(r, "EXPIRE", "b", "200", "NX").I)
	require.Equal(t, int64(1), dispatch(r, "EXPIRE", "b", "200", "XX").I)
}

// Given: key 过期时间为 now+1000s
// When: EXPIRE k 10 GT / EXPIRE k 100000 GT / EXPIRE k 10 LT / EXPIRE k 100000 LT
// Then: GT 仅新值更大时设置，LT 仅新值更小时设置
func Test_Expire_when_GtLt(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v")
	base := time.Now().Add(1000 * time.Second).Unix()
	require.Equal(t, int64(1), dispatch(r, "EXPIREAT", "k", itoa(base)).I)
	require.Equal(t, int64(0), dispatch(r, "EXPIRE", "k", "10", "GT").I)
	require.Equal(t, int64(1), dispatch(r, "EXPIRE", "k", "100000", "GT").I)
	require.Equal(t, int64(1), dispatch(r, "EXPIRE", "k", "10", "LT").I)
	require.Equal(t, int64(0), dispatch(r, "EXPIRE", "k", "100000", "LT").I)
}

// Given: 短字符串与长字符串 key
// When: OBJECT ENCODING key
// Then: 短串 embstr，长串 raw；缺失 key 返回 nil
func Test_Object_when_Encoding(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "short", "v")
	require.Equal(t, "embstr", string(dispatch(r, "OBJECT", "ENCODING", "short").Bulk))
	long := make([]byte, 64)
	for i := range long {
		long[i] = 'x'
	}
	dispatch(r, "SET", "long", string(long))
	require.Equal(t, "raw", string(dispatch(r, "OBJECT", "ENCODING", "long").Bulk))
	got := dispatch(r, "OBJECT", "ENCODING", "nope")
	require.Nil(t, got.Bulk)
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
