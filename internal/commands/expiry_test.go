package commands

import (
	"context"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

// Given: 一个已过期和一个未过期的 key
// When: SweepOnce
// Then: 只删过期 key，返回删除数 1，未过期保留
func Test_Expirer_when_SweepOnce(t *testing.T) {
	r, store, _ := openMonitorSetup(t)
	dispatch(r, "SET", "old", "v", "EXAT", "1")
	dispatch(r, "SET", "fresh", "v", "EX", "100")

	ex := NewExpirer(store, nil)
	n, err := ex.SweepOnce()
	require.NoError(t, err)
	require.Equal(t, 1, n)
	got := dispatch(r, "EXISTS", "fresh")
	require.Equal(t, int64(1), got.I)
}

// Given: 带 1 秒 TTL 的 key
// When: 等待过期后 GET
// Then: 被动删除，返回 nil
func Test_Expire_when_LazyDeletes(t *testing.T) {
	r, _, _ := openMonitorSetup(t)
	dispatch(r, "SET", "k", "v", "EX", "1")
	time.Sleep(1100 * time.Millisecond)
	got := dispatch(r, "GET", "k")
	require.Nil(t, got.Bulk)
}

// Given: 带 1 秒 TTL 的 key，无任何读操作
// When: 后台清扫运行
// Then: store 层 key 消失（清扫器删的，不是读删的），删除复制到 hub
func Test_Expirer_when_BackgroundCleans(t *testing.T) {
	ctx := context.Background()
	hub := replication.NewHub(16)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: true, Fsync: storage.FsyncNo, Hub: hub})
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.Set(ctx, []byte("s:k"),
		datastruct.Encode(datastruct.TypeString, time.Now().Add(time.Second).UnixNano(), []byte("v"))))
	ex := NewExpirer(store, nil)
	ex.Start()
	defer ex.Stop()
	require.Eventually(t, func() bool {
		_, err := store.Get(ctx, []byte("s:k"))
		return err != nil
	}, 5*time.Second, 50*time.Millisecond)

	ops, err := hub.Backlog().Since(0)
	require.NoError(t, err)
	require.NotEmpty(t, ops)
	require.True(t, ops[len(ops)-1].Del)
}
