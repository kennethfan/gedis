package commands

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/storage"
)

func openHashBench(b *testing.B, r *network.Router) {
	b.Helper()
	dispatch(r, "HSET", "bench", "f", "v")
}

func Benchmark_HSET(b *testing.B) {
	store := storage.New(b.TempDir())
	if err := store.Open(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	r := network.NewRouter()
	RegisterHash(r, store)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		r.Dispatch(ctx, cmd("HSET", "bench", "f", "v"))
	}
}

func Benchmark_HGET(b *testing.B) {
	store := storage.New(b.TempDir())
	if err := store.Open(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	r := network.NewRouter()
	RegisterHash(r, store)
	ctx := context.Background()
	openHashBench(b, r)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Dispatch(ctx, cmd("HGET", "bench", "f"))
	}
}
