package commands

import (
	"testing"
)

func Benchmark_Set(b *testing.B) {
	r, _ := openTestSetup(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dispatch(r, "SET", "k", "v")
	}
}

func Benchmark_Get(b *testing.B) {
	r, _ := openTestSetup(b)
	dispatch(r, "SET", "k", "v")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dispatch(r, "GET", "k")
	}
}
