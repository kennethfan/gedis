package protocol

import (
	"bufio"
	"strings"
	"testing"
)

var benchCmd = "*3\r\n$3\r\nSET\r\n$7\r\nmykey12\r\n$6\r\nmyval1\r\n"

func BenchmarkDecode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := Decode(bufio.NewReader(strings.NewReader(benchCmd)))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend(b *testing.B) {
	v, _ := Decode(bufio.NewReader(strings.NewReader(benchCmd)))
	b.ResetTimer()
	dst := make([]byte, 0, 64)
	for i := 0; i < b.N; i++ {
		dst = v.Append(dst[:0])
	}
}
