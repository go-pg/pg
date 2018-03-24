package orm

import (
	"fmt"
	"reflect"
	"sync"
)

var Tables = newTables()

type tables struct {
	mu     sync.RWMutex
	tables map[reflect.Type]*Table
}

func newTables() *tables {
	return &tables{
		tables: make(map[reflect.Type]*Table),
	}
}

func (t *tables) Register(strct interface{}) {
	typ := reflect.TypeOf(strct)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	_ = t.Get(typ)
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
	table, ok = t.tables[typ]
	if !ok {
		table = &Table{
			Type: typ,
		}
		t.tables[typ] = table
	}
	t.mu.Unlock()

	if !ok {
		table.init()
	}

	return table
}

func (t *tables) GetByName(name string) *Table {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, t := range t.tables {
		if string(t.Name) == name {
			return t
		}
	}

	return nil
}
