package orm

import (
	"reflect"
	"sync"
)

var registry = newTypeRegistry()

type typeRegistry struct {
	tables    map[reflect.Type]*Table
	tablesMtx sync.RWMutex
}

func newTypeRegistry() *typeRegistry {
	return &typeRegistry{
		tables: make(map[reflect.Type]*Table),
	}
}

func (c *typeRegistry) Table(typ reflect.Type) *Table {
	c.tablesMtx.RLock()
	table, ok := c.tables[typ]
	c.tablesMtx.RUnlock()
	if ok {
		return table
	}

	table = NewTable(typ)

	c.tablesMtx.Lock()
	if _, ok = c.tables[typ]; !ok {
		c.tables[typ] = table
	}
	c.tablesMtx.Unlock()

	return table
}
