package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/stretchr/testify/require"
)

// 防腐：Router 上每个已注册命令必须在 arity 表中有条目，新增命令漏加直接红。
func Test_CommandArityCoversRouter(t *testing.T) {
	_, store, stats, hub := openReplSetup(t)
	r := network.DefaultRouter()
	r.AttachStats(stats)
	RegisterStrings(r, store)
	RegisterHash(r, store)
	RegisterList(r, store, stats)
	RegisterSet(r, store)
	RegisterZSet(r, store)
	RegisterGeo(r, store)
	RegisterScan(r, store)
	RegisterGeneric(r, store)
	RegisterBitmap(r, store)
	RegisterHLL(r, store)
	RegisterStream(r, store, stats)
	RegisterMonitor(r, store, stats, hub)
	RegisterReplication(r, store, stats, hub)
	RegisterWriteCommands(r)
	RegisterTxn(r, hub)
	table := CommandArity()
	for _, name := range r.Commands() {
		_, ok := table[name]
		require.True(t, ok, "missing arity entry for %s", name)
	}
}
