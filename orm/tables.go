package orm

import (
	"fmt"
	"reflect"
	"sync"
)

var _tables = newTables()

// GetTable returns a Table for a struct type.
func GetTable(typ reflect.Type) *Table {
	return _tables.Get(typ)
}

// RegisterTable registers a struct as SQL table.
// It is usually used to register intermediate table
// in many to many relationship.
func RegisterTable(strct interface{}) {
	_tables.Register(strct)
}

type tables struct {
	mu         sync.RWMutex
	inProgress map[reflect.Type]*Table
	tables     map[reflect.Type]*Table
}

func newTables() *tables {
	return &tables{
		inProgress: make(map[reflect.Type]*Table),
		tables:     make(map[reflect.Type]*Table),
	}
}

func (t *tables) Register(strct interface{}) {
	typ := reflect.TypeOf(strct)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	_ = t.Get(typ)
}

func (t *tables) get(typ reflect.Type, inProgress bool) *Table {
	if typ.Kind() != reflect.Struct {
		panic(fmt.Errorf("got %s, wanted %s", typ.Kind(), reflect.Struct))
	}

	t.mu.RLock()
	table, ok := t.tables[typ]
	t.mu.RUnlock()
	if ok {
		return table
	}

	var dup bool
	t.mu.Lock()
	table, ok = t.tables[typ]
	if !ok {
		if inProgress {
			table, ok = t.inProgress[typ]
		}
		if !ok {
			table = newTable(typ)
			_, dup = t.inProgress[typ]
			if !dup {
				t.inProgress[typ] = table
			}
		}
	}
	t.mu.Unlock()

	if !ok {
		table.init()
		if !dup {
			t.mu.Lock()
			delete(t.inProgress, typ)
			t.tables[typ] = table
			t.mu.Unlock()
		}
	}

	return table
}

func (t *tables) Get(typ reflect.Type) *Table {
	return t.get(typ, false)
}

func (t *tables) getByName(name string) *Table {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, t := range t.tables {
		if string(t.Name) == name {
			return t
		}
	}

	return nil
}
