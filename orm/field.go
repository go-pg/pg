package orm

import (
	"reflect"

	"gopkg.in/pg.v3/types"
)

const (
	NullEmptyFlag  = 1 << 0
	PrimaryKeyFlag = 1 << 1
	ForeignKeyFlag = 1 << 2
)

type Field struct {
	GoName  string
	SQLName string
	Index   []int

	flags int8

	appender func([]byte, reflect.Value, bool) []byte
	decoder  func(reflect.Value, []byte) error
}

func (f *Field) Copy() *Field {
	copy := *f
	copy.Index = copy.Index[:len(f.Index):len(f.Index)]
	return &copy
}

func (f *Field) Has(flag int8) bool {
	return f.flags&flag != 0
}

func (f *Field) IsEmpty(v reflect.Value) bool {
	fv := v.FieldByIndex(f.Index)
	return isEmptyValue(fv)
}

func (f *Field) AppendValue(dst []byte, v reflect.Value, quote bool) []byte {
	fv := v.FieldByIndex(f.Index)
	if f.Has(NullEmptyFlag) && isEmptyValue(fv) {
		return types.AppendNull(dst, quote)
	}
	return f.appender(dst, fv, quote)
}

func (f *Field) DecodeValue(v reflect.Value, b []byte) error {
	v = fieldByIndex(v, f.Index)
	if b == nil {
		return types.DecodeNullValue(v)
	}
	return f.decoder(v, b)
}

func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	if len(index) == 1 {
		return v.Field(index[0])
	}
	for i, x := range index {
		if i > 0 && v.Kind() == reflect.Ptr {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(x)
	}
	return v
}

type method struct {
	Index int

	appender func([]byte, reflect.Value, bool) []byte
}

func (m *method) AppendValue(dst []byte, v reflect.Value, quote bool) []byte {
	mv := v.Method(m.Index).Call(nil)[0]
	return m.appender(dst, mv, quote)
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}
