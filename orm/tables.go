package orm

import (
	"fmt"
	"reflect"
	"sync"
)

var _tables = newTables()

type tableInProgress struct {
	table *Table

	init1Once sync.Once
	init1WG   sync.WaitGroup

	init2Once sync.Once
	init2WG   sync.WaitGroup
}

func newTableInProgress(table *Table) *tableInProgress {
	inp := &tableInProgress{
		table: table,
	}
	inp.init1WG.Add(1)
	inp.init2WG.Add(1)
	return inp
}

func (inp *tableInProgress) init1() bool {
	var inited bool
	inp.init1Once.Do(func() {
		inp.table.init1()
		inp.init1WG.Done()
		inited = true
	})
	inp.init1WG.Wait()
	return inited
}

func (inp *tableInProgress) init2() bool {
	var inited bool
	inp.init2Once.Do(func() {
		inp.table.init2()
		inp.init2WG.Done()
		inited = true
	})
	inp.init2WG.Wait()
	return inited
}

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
	inProgress map[reflect.Type]*tableInProgress
	tables     map[reflect.Type]*Table
}

func newTables() *tables {
	return &tables{
		inProgress: make(map[reflect.Type]*tableInProgress),
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

func (t *tables) get(typ reflect.Type, allowInProgress bool) *Table {
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
	if ok {
		t.mu.Unlock()
		return table
	}

	inProgress := t.inProgress[typ]
	if inProgress == nil {
		table = newTable(typ)
		inProgress = newTableInProgress(table)
		t.inProgress[typ] = inProgress
	} else {
		table = inProgress.table
	}

	t.mu.Unlock()

	inProgress.init1()
	if allowInProgress {
		return table
	}

	if inProgress.init2() {
		t.mu.Lock()
		delete(t.inProgress, typ)
		t.tables[typ] = table
		t.mu.Unlock()
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
		if string(t.FullName) == name || t.ModelName == name {
			return t
		}
	}

	return nil
}
