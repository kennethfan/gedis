package storage

import (
	"context"
	"fmt"
	"testing"
)

func benchWritePolicy(b *testing.B, policy FsyncPolicy) {
	b.Helper()
	ctx := context.Background()
	p := NewWithOptions(b.TempDir(), Options{AppendOnly: true, Fsync: policy})
	if err := p.Open(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Set(ctx, []byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_WriteNo(b *testing.B)       { benchWritePolicy(b, FsyncNo) }
func Benchmark_WriteEverysec(b *testing.B) { benchWritePolicy(b, FsyncEverysec) }
func Benchmark_WriteAlways(b *testing.B)   { benchWritePolicy(b, FsyncAlways) }
