package orm

import (
	"fmt"
	"reflect"
	"sync"
)

var Tables = newTables()

type tables struct {
	inFlight map[reflect.Type]*Table
	tables   map[reflect.Type]*Table
	mu       sync.RWMutex
}

func newTables() *tables {
	return &tables{
		inFlight: make(map[reflect.Type]*Table),
		tables:   make(map[reflect.Type]*Table),
	}
}

func (t *tables) Get(typ reflect.Type) *Table {
	if typ.Kind() != reflect.Struct {
		panic(fmt.Errorf("got %s, wanted %s", typ.Kind(), reflect.Struct))
	}

	t.mu.RLock()
	table, ok := t.tables[typ]
	t.mu.RUnlock()
	if ok {
		return table
	}
	t.mu.Lock()
	table = newTable(typ)
	t.mu.Unlock()
	return table
}
