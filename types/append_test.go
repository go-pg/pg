package types

import (
	"testing"
	"unicode/utf8"
)

func BenchmarkAppendRuneOld(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := make([]byte, 0, 1024)
		b.StartTimer()

		for j := 0; j < 1000000; j++ {
			s = appendRuneOld(s, '世')
		}
	}
}

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

func appendRuneOld(b []byte, r rune) []byte {
	if r < utf8.RuneSelf {
		return append(b, byte(r))
	}
	l := len(b)
	if cap(b)-l < utf8.UTFMax {
		b = append(b, make([]byte, utf8.UTFMax)...)
	}
	n := utf8.EncodeRune(b[l:l+utf8.UTFMax], r)
	return b[:l+n]
}
