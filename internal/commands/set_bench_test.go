package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/storage"
)

func Benchmark_SADD(b *testing.B) {
	store := storage.New(b.TempDir())
	if err := store.Open(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	r := network.NewRouter()
	RegisterSet(r, store)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		r.Dispatch(ctx, cmd("SADD", "bench", fmt.Sprintf("m%d", i)))
	}
}

func Benchmark_SMEMBERS(b *testing.B) {
	store := storage.New(b.TempDir())
	if err := store.Open(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	r := network.NewRouter()
	RegisterSet(r, store)
	ctx := context.Background()
	r.Dispatch(ctx, cmd("SADD", "bench", "a", "b", "c"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Dispatch(ctx, cmd("SMEMBERS", "bench"))
	}
}
