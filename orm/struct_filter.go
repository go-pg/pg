package orm

import (
	"reflect"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/struct_filter"
)

type structFilter struct {
	value reflect.Value         // reflect.Struct
	strct *struct_filter.Struct // lazy
}

func newStructFilter(v interface{}) *structFilter {
	if v, ok := v.(*structFilter); ok {
		return v
	}
	return &structFilter{
		value: reflect.Indirect(reflect.ValueOf(v)),
	}
}

func (sf *structFilter) init() {
	if sf.strct == nil {
		sf.strct = struct_filter.GetStruct(sf.value.Type())
	}
}

func (sf *structFilter) Where() string {
	const and = " AND "

	sf.init()

	var b []byte
	for _, f := range sf.strct.Fields {
		fv := f.Value(sf.value)
		if f.Omit(fv) {
			continue
		}

		if b != nil {
			b = append(b, and...)
		}
		b = f.Append(b, fv)
	}

	return internal.BytesToString(b)
}
