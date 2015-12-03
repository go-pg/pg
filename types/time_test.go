package types_test

import (
	"testing"

	"gopkg.in/pg.v4/types"
)

func BenchmarkParseTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		types.ParseTime([]byte("2001-02-03 04:05:06+07"))
	}
}
