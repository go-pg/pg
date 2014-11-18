package pg

import (
	"reflect"
	"strings"
	"sync"

	"gopkg.in/pg.v3/pgutil"
)

var (
	structs = newStructCache()
)

type structCache struct {
	fields  map[reflect.Type]map[string][]int
	fieldsl sync.RWMutex

	methods  map[reflect.Type]map[string]int
	methodsl sync.RWMutex
}

func newStructCache() *structCache {
	return &structCache{
		fields:  make(map[reflect.Type]map[string][]int),
		methods: make(map[reflect.Type]map[string]int),
	}
}

func (c *structCache) Fields(typ reflect.Type) map[string][]int {
	c.fieldsl.RLock()
	indxs, ok := c.fields[typ]
	c.fieldsl.RUnlock()
	if ok {
		return indxs
	}

	indxs = fields(typ)

	c.fieldsl.Lock()
	c.fields[typ] = indxs
	c.fieldsl.Unlock()

	return indxs
}

func (c *structCache) Methods(typ reflect.Type) map[string]int {
	c.methodsl.RLock()
	indxs, ok := c.methods[typ]
	c.methodsl.RUnlock()
	if ok {
		return indxs
	}

	num := typ.NumMethod()
	indxs = make(map[string]int, num)
	for i := 0; i < num; i++ {
		m := typ.Method(i)
		if m.Type.NumIn() > 1 {
			continue
		}
		if m.Type.NumOut() != 1 {
			continue
		}
		indxs[m.Name] = m.Index
	}

	c.methodsl.Lock()
	c.methods[typ] = indxs
	c.methodsl.Unlock()

	return indxs
}

func fields(typ reflect.Type) map[string][]int {
	num := typ.NumField()
	dst := make(map[string][]int, num)
	for i := 0; i < num; i++ {
		f := typ.Field(i)

		if f.Anonymous {
			typ := f.Type
			if typ.Kind() == reflect.Ptr {
				typ = typ.Elem()
			}
			for name, indx := range fields(typ) {
				dst[name] = append(f.Index, indx...)
			}
			continue
		}

		if f.PkgPath != "" {
			continue
		}

		tokens := strings.Split(f.Tag.Get("pg"), ",")
		name := tokens[0]
		if name == "-" {
			continue
		}
		if name == "" {
			name = pgutil.Underscore(f.Name)
		}

		tt := indirectType(f.Type)
		if tt.Kind() == reflect.Struct {
			for subname, indx := range fields(tt) {
				dst[name+"__"+subname] = append(f.Index, indx...)
			}
		}

		dst[name] = f.Index
	}
	return dst
}

func indirectType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}
