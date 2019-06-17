package orm

import (
	"reflect"
	"testing"
)

var tableSink *Table

func BenchmarkTablesGet(b *testing.B) {
	tables := newTables()
	typ := reflect.TypeOf(Query{})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tableSink = tables.Get(typ)
		}
	})
}
