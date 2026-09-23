package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: ">" 已投递 3 条给 c1
// When: XCLAIM 认领先 2 条
// Then: 返完整条目 + count=2 + 属主迁移
func Test_Stream_when_ClaimBasic(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XADD", "s", "100-3", "a", "3")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0")
	dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	got := dispatch(r, "XCLAIM", "s", "g", "c2", "0", "100-1", "100-2")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.Equal(t, "100-1", string(got.Elems[0].Elems[0].Bulk))
	require.Equal(t, "100-2", string(got.Elems[1].Elems[0].Bulk))
	// c2 pending 2 条且 count=2。
	got = dispatch(r, "XPENDING", "s", "g", "-", "+", "10", "c2")
	require.Len(t, got.Elems, 2)
	require.Equal(t, int64(2), got.Elems[0].Elems[3].I)
	// c1 只剩 100-3。
	got = dispatch(r, "XPENDING", "s", "g", "-", "+", "10", "c1")
	require.Len(t, got.Elems, 1)
	require.Equal(t, "100-3", string(got.Elems[0].Elems[0].Bulk))
}

// Given: 从未投递的 entry
// When: 非 FORCE / FORCE / IDLE / RETRYCOUNT / JUSTID / TIME
// Then: 非 FORCE 空；FORCE 建 PEL count=1；IDLE/RETRYCOUNT 绝对设定；
// JUSTID 只迁移 owner，不碰 time/count
func Test_Stream_when_ClaimForceIdleRetry(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0")
	got := dispatch(r, "XCLAIM", "s", "g", "c9", "0", "100-1")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 0)
	got = dispatch(r, "XCLAIM", "s", "g", "c9", "0", "100-1", "FORCE")
	require.Len(t, got.Elems, 1)
	got = dispatch(r, "XPENDING", "s", "g", "100-1", "100-1", "1")
	require.Equal(t, int64(1), got.Elems[0].Elems[3].I)
	// IDLE 5000 → idle≈5000 且 count++（1→2）；RETRYCOUNT 7 → count=7 不 bump。
	dispatch(r, "XCLAIM", "s", "g", "c9", "0", "100-1", "IDLE", "5000")
	got = dispatch(r, "XPENDING", "s", "g", "100-1", "100-1", "1")
	require.GreaterOrEqual(t, got.Elems[0].Elems[2].I, int64(4990))
	require.Equal(t, int64(2), got.Elems[0].Elems[3].I)
	dispatch(r, "XCLAIM", "s", "g", "c9", "0", "100-1", "RETRYCOUNT", "7")
	got = dispatch(r, "XPENDING", "s", "g", "100-1", "100-1", "1")
	require.Equal(t, int64(7), got.Elems[0].Elems[3].I)
	got = dispatch(r, "XCLAIM", "s", "g", "c9", "0", "100-1", "JUSTID")
	require.Len(t, got.Elems, 1)
	require.Equal(t, "100-1", string(got.Elems[0].Bulk))
	// JUSTID 后 count 仍 7。
	got = dispatch(r, "XPENDING", "s", "g", "100-1", "100-1", "1")
	require.Equal(t, int64(7), got.Elems[0].Elems[3].I)
	// FORCE 对流外 ID 仍空。
	got = dispatch(r, "XCLAIM", "s", "g", "c9", "0", "999-9", "FORCE")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 0)
}

// Given: 各类坏参
// When: XCLAIM 错误形
// Then: 文本与真 Redis 一致
func Test_Stream_when_ClaimErrors(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	got := dispatch(r, "XCLAIM", "s", "g", "c")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments for 'xclaim' command")
	got = dispatch(r, "XCLAIM", "s", "g", "c", "x", "100-1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Invalid min-idle-time argument for XCLAIM")
	got = dispatch(r, "XCLAIM", "s", "g", "c", "0", "bad")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Unrecognized XCLAIM option 'bad'")
	got = dispatch(r, "XCLAIM", "missing", "g", "c", "0", "100-1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOGROUP")
	got = dispatch(r, "XCLAIM", "s", "nog", "c", "0", "100-1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOGROUP")
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0")
	got = dispatch(r, "XCLAIM", "s", "g", "c", "0", "100-1", "IDLE", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Invalid IDLE option argument for XCLAIM")
	got = dispatch(r, "XCLAIM", "s", "g", "c", "0", "100-1", "RETRYCOUNT", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Invalid RETRYCOUNT option argument for XCLAIM")
	got = dispatch(r, "XCLAIM", "s", "g", "c", "0", "100-1", "TIME", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Invalid TIME option argument for XCLAIM")
	got = dispatch(r, "XCLAIM", "s", "g", "c", "0", "100-1", "BOGUS")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Unrecognized XCLAIM option 'BOGUS'")
	// IDLE/RETRYCOUNT 负数照收。
	got = dispatch(r, "XCLAIM", "s", "g", "c", "0", "100-1", "FORCE", "IDLE", "-3", "RETRYCOUNT", "-5")
	require.Equal(t, protocol.KindArray, got.Kind)
}

// Given: PEL 有 3 条（c1）
// When: XAUTOCLAIM 扫描认领
// Then: 三元 shape + cursor 推进 + orphan 处理
func Test_Stream_when_AutoClaim(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XADD", "s", "100-3", "a", "3")
	dispatch(r, "XGROUP", "CREATE", "s", "g", "0")
	dispatch(r, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	// start 含端：COUNT 2 取前 2，cursor=100-3。
	got := dispatch(r, "XAUTOCLAIM", "s", "g", "c2", "0", "0-0", "COUNT", "2")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 3)
	require.Equal(t, "100-3", string(got.Elems[0].Bulk))
	require.Len(t, got.Elems[1].Elems, 2)
	require.Equal(t, protocol.KindArray, got.Elems[2].Kind)
	require.Len(t, got.Elems[2].Elems, 0)
	// 扫完 → cursor 0-0。
	got = dispatch(r, "XAUTOCLAIM", "s", "g", "c2", "0", "100-3", "COUNT", "10")
	require.Equal(t, "0-0", string(got.Elems[0].Bulk))
	require.Len(t, got.Elems[1].Elems, 1)
	// XDEL 后 orphaned 为裸 ID 且移出 PEL。
	dispatch(r, "XDEL", "s", "100-2")
	got = dispatch(r, "XAUTOCLAIM", "s", "g", "c1", "0", "0-0", "COUNT", "10")
	require.Len(t, got.Elems[1].Elems, 2)
	require.Len(t, got.Elems[2].Elems, 1)
	require.Equal(t, "100-2", string(got.Elems[2].Elems[0].Bulk))
	got = dispatch(r, "XPENDING", "s", "g", "-", "+", "10")
	for _, e := range got.Elems {
		require.NotEqual(t, "100-2", string(e.Elems[0].Bulk))
	}
	// JUSTID 形。
	got = dispatch(r, "XAUTOCLAIM", "s", "g", "c1", "0", "0-0", "COUNT", "10", "JUSTID")
	require.Len(t, got.Elems, 3)
	require.Equal(t, "100-1", string(got.Elems[1].Elems[0].Bulk))
}

// Given: 各类坏参
// When: XAUTOCLAIM 错误形
// Then: 文本与真 Redis 一致
func Test_Stream_when_AutoClaimErrors(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	got := dispatch(r, "XAUTOCLAIM", "s", "g", "c", "0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments for 'xautoclaim' command")
	got = dispatch(r, "XAUTOCLAIM", "s", "g", "c", "0", "0-0", "COUNT", "0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "COUNT must be > 0")
	got = dispatch(r, "XAUTOCLAIM", "s", "g", "c", "x", "0-0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Invalid min-idle-time argument for XAUTOCLAIM")
	got = dispatch(r, "XAUTOCLAIM", "missing", "g", "c", "0", "0-0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOGROUP")
	got = dispatch(r, "XAUTOCLAIM", "s", "g", "c", "0", "$")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Invalid stream ID")
}
