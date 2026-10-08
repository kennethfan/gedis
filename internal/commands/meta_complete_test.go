package commands

import (
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/sentinel"
	"github.com/kennethfan/gedis/internal/storage"
)

// 二期 Task1：全命令 Meta 零遗漏。接线顺序复刻 main.go（主 router + 哨兵 router），
// 任一 Commands() 条目无 Meta 即 FAIL，补登记后方能通过。
func wireMainRouter(t *testing.T) *network.Router {
	t.Helper()
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: false})
	if err := store.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	stats := network.NewStats()
	hub := replication.NewHub(1024)
	r := network.DefaultRouter()
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
	RegisterPubSub(r)
	RegisterLua(r, 5*time.Second)
	RegisterCluster(r, store, cluster.NewTopology(false, nil), NewAskRegistry())
	st := acl.NewStore()
	reg := RegisterAuth(r, st)
	connReg := RegisterConn(r, st, reg)
	RegisterServer(r, store, stats, hub, connReg, ServerDeps{})
	RegisterACL(r, st, reg, nil)
	return r
}

func TestMetaComplete(t *testing.T) {
	r := wireMainRouter(t)
	sRouter := network.DefaultRouter()
	RegisterPubSub(sRouter)
	RegisterSentinel(sRouter, sentinel.NewRegistry(nil, time.Second), "127.0.0.1:0", RegisterPubSub(sRouter))

	var missing []string
	seen := map[string]bool{}
	for _, router := range []*network.Router{r, sRouter} {
		for _, c := range router.Commands() {
			if seen[c] {
				continue
			}
			seen[c] = true
			if _, ok := acl.LookupMeta(c); !ok {
				missing = append(missing, c)
			}
		}
	}
	if len(missing) > 0 {
		t.Fatalf("commands without Meta: %v", missing)
	}
}
