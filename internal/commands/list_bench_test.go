package commands

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
)

func openListBench(b *testing.B) (*network.Router, *storage.Pebble) {
	b.Helper()
	store := storage.New(b.TempDir())
	if err := store.Open(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	r := network.NewRouter()
	RegisterStrings(r, store)
	RegisterList(r, store, nil)
	return r, store
}

func benchDispatch(b *testing.B, r *network.Router, args ...string) {
	b.Helper()
	elems := make([]protocol.Value, len(args))
	for i, a := range args {
		elems[i] = protocol.BulkOf(a)
	}
	r.Dispatch(context.Background(), protocol.Value{Kind: protocol.KindArray, Elems: elems})
}

func Benchmark_List_Rpush(b *testing.B) {
	r, _ := openListBench(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchDispatch(b, r, "RPUSH", "bench", "v")
	}
}

func Benchmark_List_Lpop(b *testing.B) {
	r, _ := openListBench(b)
	for i := 0; i < b.N; i++ {
		benchDispatch(b, r, "RPUSH", "bench", "v")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchDispatch(b, r, "LPOP", "bench")
		benchDispatch(b, r, "RPUSH", "bench", "v")
	}
}
