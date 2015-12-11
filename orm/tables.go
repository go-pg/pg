package orm

import (
	"reflect"
	"sync"
)

var Tables = newTables()

type tables struct {
	tables map[reflect.Type]*Table
	mtx    sync.RWMutex
}

func newTables() *tables {
	return &tables{
		tables: make(map[reflect.Type]*Table),
	}
}

func (t *tables) Get(typ reflect.Type) *Table {
	t.mtx.RLock()
	table, ok := t.tables[typ]
	t.mtx.RUnlock()
	if ok {
		return table
	}

	table = NewTable(typ)

	t.mtx.Lock()
	if _, ok = t.tables[typ]; !ok {
		t.tables[typ] = table
	}
	t.mtx.Unlock()

	return table
}
