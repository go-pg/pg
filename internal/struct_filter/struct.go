package struct_filter

import (
	"reflect"
)

type Struct struct {
	Fields []*Field
}

func NewStruct(typ reflect.Type) *Struct {
	s := &Struct{
		Fields: make([]*Field, 0, typ.NumField()),
	}

	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		if sf.Anonymous {
			continue
		}

		f := newField(sf)
		if f == nil {
			continue
		}
		s.Fields = append(s.Fields, f)
	}

	return s
}

func (s *Struct) Field(name string) *Field {
	col, opCode, _ := splitColumnOperator(name, "__")
	for _, f := range s.Fields {
		if f.column == col && f.opCode == opCode {
			return f
		}
	}
	return nil
}
