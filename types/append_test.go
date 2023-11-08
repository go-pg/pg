package types

import (
	"testing"
)

func BenchmarkAppendRuneNew(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := make([]byte, 0, 1024)
		b.StartTimer()

		for j := 0; j < 1000000; j++ {
			s = appendRune(s, '世')
		}
	}
}
