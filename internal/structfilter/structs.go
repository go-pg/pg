package structfilter

import (
	"fmt"
	"reflect"
	"sync"
)

var globalStructs = newStructs()

func GetStruct(typ reflect.Type) *Struct {
	return globalStructs.Get(typ)
}

type structs struct {
	structs sync.Map
}

func newStructs() *structs {
	return &structs{}
}

func (s *structs) Get(typ reflect.Type) *Struct {
	if typ.Kind() != reflect.Struct {
		panic(fmt.Errorf("got %s, wanted %s", typ.Kind(), reflect.Struct))
	}

	if v, ok := s.structs.Load(typ); ok {
		return v.(*Struct)
	}

	strct := NewStruct(typ)
	if v, loaded := s.structs.LoadOrStore(typ, strct); loaded {
		return v.(*Struct)
	}
	return strct
}
