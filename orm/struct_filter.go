package orm

import (
	"reflect"
	"sync"

	"github.com/go-pg/pg/internal/struct_filter"
)

type structFilter struct {
	value reflect.Value // reflect.Struct

	strctOnce sync.Once
	strct     *struct_filter.Struct // lazy
}

var _ sepFormatAppender = (*structFilter)(nil)

func newStructFilter(v interface{}) *structFilter {
	if v, ok := v.(*structFilter); ok {
		return v
	}
	return &structFilter{
		value: reflect.Indirect(reflect.ValueOf(v)),
	}
}

func (sf *structFilter) AppendSep(b []byte) []byte {
	return append(b, " AND "...)
}

func (sf *structFilter) AppendFormat(b []byte, fmter QueryFormatter) []byte {
	sf.strctOnce.Do(func() {
		sf.strct = struct_filter.GetStruct(sf.value.Type())
	})

	isPlaceholder := isPlaceholderFormatter(fmter)

	before := len(b)
	for _, f := range sf.strct.Fields {
		fv := f.Value(sf.value)
		if f.Omit(fv) {
			continue
		}

		if len(b) != before {
			b = append(b, " AND "...)
		}

		if sf.strct.TableName != "" {
			b = append(b, sf.strct.TableName...)
			b = append(b, '.')
		}
		b = append(b, f.Column...)
		b = append(b, f.OpValue...)
		if f.IsSlice {
			b = append(b, '(')
		}
		if isPlaceholder {
			b = append(b, '?')
		} else {
			b = f.Append(b, fv, 1)
		}
		if f.IsSlice {
			b = append(b, ')')
		}
	}

	return b
}
