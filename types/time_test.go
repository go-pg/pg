package types_test

import (
	"testing"

	"github.com/go-pg/pg/types"
)

func BenchmarkParseTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		types.ParseTime([]byte("2001-02-03 04:05:06+07"))
	}
}
