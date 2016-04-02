package orm

import (
	"reflect"

	"gopkg.in/pg.v4/types"
)

var (
	queryType = reflect.TypeOf(types.Q(nil))
	fieldType = reflect.TypeOf(types.F(""))
)

const (
	PrimaryKeyFlag = 1 << iota
	ForeignKeyFlag = 1 << iota
	NullEmptyFlag  = 1 << iota
	FormatFlag     = 1 << iota
)

type Field struct {
	GoName  string
	SQLName string
	Index   []int

	flags int8

	append types.AppenderFunc
	scan   types.ScannerFunc

	isEmpty isEmptyFunc
}

func (f *Field) Copy() *Field {
	copy := *f
	copy.Index = copy.Index[:len(f.Index):len(f.Index)]
	return &copy
}

func (f *Field) Has(flag int8) bool {
	return f.flags&flag != 0
}

func (f *Field) Value(strct reflect.Value) reflect.Value {
	return strct.FieldByIndex(f.Index)
}

func (f *Field) IsEmpty(strct reflect.Value) bool {
	fv := f.Value(strct)
	return f.isEmpty(fv)
}

func (f *Field) AppendValue(b []byte, strct reflect.Value, quote int) []byte {
	fv := f.Value(strct)
	if f.Has(NullEmptyFlag) && f.isEmpty(fv) {
		return types.AppendNull(b, quote)
	}
	return f.append(b, fv, quote)
}

func (f *Field) ScanValue(strct reflect.Value, b []byte) error {
	fv := fieldByIndex(strct, f.Index)
	return f.scan(fv, b)
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

	flags int8

	appender func([]byte, reflect.Value, int) []byte
}

func (m *method) Has(flag int8) bool {
	return m.flags&flag != 0
}

func (m *method) Value(strct reflect.Value) reflect.Value {
	return strct.Method(m.Index).Call(nil)[0]
}

func (m *method) AppendValue(dst []byte, strct reflect.Value, quote int) []byte {
	mv := m.Value(strct)
	return m.appender(dst, mv, quote)
}
