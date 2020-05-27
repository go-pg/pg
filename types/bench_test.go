package types

import (
	"reflect"
	"testing"
)

func BenchmarkAppendArrayBytesValue(b *testing.B) {
	var bytes [64]byte
	v := reflect.ValueOf(bytes)

	for i := 0; i < b.N; i++ {
		_ = appendArrayBytesValue(nil, v, 0)
	}
}
