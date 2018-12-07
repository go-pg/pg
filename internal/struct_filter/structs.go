package struct_filter

import (
	"fmt"
	"reflect"
	"sync"
)

var _structs = newStructs()

func GetStruct(typ reflect.Type) *Struct {
	return _structs.Get(typ)
}

type structs struct {
	mu      sync.RWMutex
	structs map[reflect.Type]*Struct
}

func newStructs() *structs {
	return &structs{
		structs: make(map[reflect.Type]*Struct),
	}
}

func (s *structs) Get(typ reflect.Type) *Struct {
	if typ.Kind() != reflect.Struct {
		panic(fmt.Errorf("got %s, wanted %s", typ.Kind(), reflect.Struct))
	}

	s.mu.RLock()
	strct, ok := s.structs[typ]
	s.mu.RUnlock()
	if ok {
		return strct
	}

	s.mu.Lock()

	strct, ok = s.structs[typ]
	if ok {
		s.mu.Unlock()
		return strct
	}

	strct = NewStruct(typ)
	s.structs[typ] = strct

	s.mu.Unlock()
	return strct
}
