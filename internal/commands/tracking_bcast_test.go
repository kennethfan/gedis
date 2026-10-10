package commands

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Given: A（RESP3）CLIENT TRACKING on BCAST PREFIX user PREFIX user:，从未 GET
//
//（重叠前缀）；B TRACKING on BCAST PREFIX user: NOLOOP；B 亦无 GET
// When: B SET user:x（A 两前缀同时命中、B 前缀命中且 NOLOOP）；B SET other:1；
//
//	B SET userx（仅命中 "user"）
// Then: A 收一次 user:x（去重不重复发）；other:1 零帧；userx 收一次；B 全程零帧
func Test_Bcast_InvalidatesByPrefix(t *testing.T) {
	r, _, h, _ := openPushSetup(t)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	h.conns.SetProto(srvA, 3)
	h.conns.SetProto(srvB, 3)

	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "on", "BCAST",
		"PREFIX", "user", "PREFIX", "user:"))
	pushOK(t, dispatchWithConn(r, srvB, "CLIENT", "TRACKING", "on", "BCAST",
		"PREFIX", "user:", "NOLOOP"))

	// BCAST 不依赖读注册：即便 GET 也不入正向表
	_ = dispatchWithConn(r, srvA, "GET", "user:regcheck")
	require.Empty(t, h.tracks.ConnsFor("user:regcheck"), "BCAST 不做读注册")

	pushOK(t, dispatchWithConn(r, srvB, "SET", "user:x", "1"))
	require.Equal(t, invalidateValue("user:x"), readPushed(t, cliA))
	expectNoFrames(t, cliA, 500*time.Millisecond, "多前缀同时命中只发一次")
	expectNoFrames(t, cliB, 300*time.Millisecond, "NOLOOP 写者零帧")

	pushOK(t, dispatchWithConn(r, srvB, "SET", "other:1", "1"))
	expectNoFrames(t, cliA, 300*time.Millisecond, "前缀不命中零帧")
	expectNoFrames(t, cliB, 100*time.Millisecond)

	pushOK(t, dispatchWithConn(r, srvB, "SET", "userx", "1"))
	require.Equal(t, invalidateValue("userx"), readPushed(t, cliA))
	expectNoFrames(t, cliA, 300*time.Millisecond)
	expectNoFrames(t, cliB, 100*time.Millisecond)
}

// Given: A OPTIN + CACHING yes（跨 conn 隔离）；A off 复位；A 再开 BCAST、
//
//	A2 读注册 foo；B 无 tracking
// When: A off；A2 ConnClosed（断连清理）；期间 B SET user:1 / foo
// Then: off 后 CachingYes 复位、BCAST 表清零通知；断连后正向表清零通知；
//
//	CachingYes 不跨 conn（B 恒 false）
func Test_Tracking_CleanupOnOffAndDisconnect(t *testing.T) {
	r, _, h, _ := openPushSetup(t)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	srvA2, cliA2 := net.Pipe()
	t.Cleanup(func() {
		srvA.Close(); cliA.Close()
		srvB.Close(); cliB.Close()
		srvA2.Close(); cliA2.Close()
	})
	h.conns.SetProto(srvA, 3)
	h.conns.SetProto(srvA2, 3)

	// OPTIN + CACHING yes：不跨 conn
	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "on", "OPTIN"))
	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "CACHING", "yes"))
	require.True(t, h.conns.CachingYesOf(srvA))
	require.False(t, h.conns.CachingYesOf(srvB), "CachingYes 不跨 conn")

	// off：CachingYes 残留复位
	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "off"))
	require.False(t, h.conns.CachingYesOf(srvA), "off 后 CachingYes 残留复位")

	// A 再开 BCAST；A2 读注册 foo
	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "on", "BCAST", "PREFIX", "user:"))
	_ = dispatchWithConn(r, srvA, "GET", "user:skip")
	require.Empty(t, h.tracks.ConnsFor("user:skip"))
	pushOK(t, dispatchWithConn(r, srvA2, "CLIENT", "TRACKING", "on"))
	_ = dispatchWithConn(r, srvA2, "GET", "foo")
	idA := h.conns.IDOf(srvA)
	idA2 := h.conns.IDOf(srvA2)
	require.Equal(t, []int64{idA}, h.tracks.BcastFor("user:1"))
	require.Equal(t, []int64{idA2}, h.tracks.ConnsFor("foo"))

	// off：BCAST 表全清，后续写零通知
	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "off"))
	require.Empty(t, h.tracks.BcastFor("user:1"), "off 清 BCAST 表")
	pushOK(t, dispatchWithConn(r, srvB, "SET", "user:1", "1"))
	expectNoFrames(t, cliA, 300*time.Millisecond, "off 后零通知")

	// 断连：正向表清，后续写零通知
	h.conns.ConnClosed(srvA2)
	require.Empty(t, h.tracks.ConnsFor("foo"), "断连清正向表")
	require.Empty(t, h.tracks.BcastFor("user:1"))
	pushOK(t, dispatchWithConn(r, srvB, "SET", "foo", "1"))
	expectNoFrames(t, cliA, 100*time.Millisecond)
}
