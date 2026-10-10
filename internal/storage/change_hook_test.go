package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type changeHookCtxKey struct{}

// Given: change hook 已注入且 ctx 携带值
// When: Set / Delete / WriteBatch 依次成功执行，随后 ctx 取消再写、hook 置 nil 再写
// Then: 每次成功变更回调收到 op 's'/'d' + raw key + 同一 ctx（值可见）；
//
//	失败路径零回调；nil hook 零回调不 panic。
func Test_SetChangeHook(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()

	type ev struct {
		op  byte
		key string
		val string
	}
	var got []ev
	p.SetChangeHook(func(c context.Context, op byte, rawKey []byte) {
		v, _ := c.Value(changeHookCtxKey{}).(string)
		got = append(got, ev{op: op, key: string(rawKey), val: v})
	})
	t.Cleanup(func() { p.SetChangeHook(nil) })

	kctx := context.WithValue(ctx, changeHookCtxKey{}, "yes")
	require.NoError(t, p.Set(kctx, []byte("s:k1"), []byte("v1")))
	require.NoError(t, p.Delete(kctx, []byte("s:k1")))
	require.NoError(t, p.WriteBatch(kctx, []BatchOp{
		{Key: []byte("s:b1"), Value: []byte("x")},
		{Key: []byte("s:b1"), Delete: true},
		{Key: []byte("s:b2"), Value: []byte("y")},
	}))
	require.Equal(t, []ev{
		{op: 's', key: "s:k1", val: "yes"},
		{op: 'd', key: "s:k1", val: "yes"},
		{op: 's', key: "s:b1", val: "yes"},
		{op: 'd', key: "s:b1", val: "yes"},
		{op: 's', key: "s:b2", val: "yes"},
	}, got, "成功变更按序回调，ctx 值透传")

	// ctx 取消：入口即失败，不写不回调
	canceled, cancel := context.WithCancel(kctx)
	cancel()
	require.Error(t, p.Set(canceled, []byte("s:k3"), []byte("v")))
	require.Error(t, p.Delete(canceled, []byte("s:k3")))
	require.Len(t, got, 5, "失败路径不得回调")

	// nil hook：零回调不 panic
	p.SetChangeHook(nil)
	require.NoError(t, p.Set(kctx, []byte("s:k4"), []byte("v")))
	require.Len(t, got, 5)
}

// Given: 上限 100B + change hook 与 evict hook 同时注入
// When: 写入超限 key 连续触发驱逐
// Then: 被逐出 key 不产生 op 'd'（逐出唯一出口是 evictHook 的 op 'e'）；
//
//	evictHook 照常收到被逐 raw key。
func Test_SetChangeHookSkipsEvictPath(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	p.SetMaxBytes(100)

	var dels []string
	p.SetChangeHook(func(_ context.Context, op byte, rawKey []byte) {
		if op == 'd' {
			dels = append(dels, string(rawKey))
		}
	})
	t.Cleanup(func() { p.SetChangeHook(nil) })
	var evicted []string
	p.SetEvictHook(func(k string) { evicted = append(evicted, k) })
	t.Cleanup(func() { p.SetEvictHook(nil) })

	for i := 0; i < 20; i++ {
		require.NoError(t, p.Set(ctx, []byte("k"+strings.Repeat("x", 3)+string(rune('a'+i))), []byte(strings.Repeat("v", 20))))
	}
	require.Greater(t, p.EvictedCount(), int64(0), "必须发生驱逐")
	require.NotEmpty(t, evicted, "evict hook 必须触发")
	require.Empty(t, dels, "逐出不得经 Delete 的 op 'd'（避免与 op 'e' 双重失效推送）")
}
