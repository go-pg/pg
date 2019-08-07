package orm

import (
	"fmt"
	"reflect"

	"github.com/whenspeakteam/pg/v9/internal/iszero"
	"github.com/whenspeakteam/pg/v9/types"
)

const (
	PrimaryKeyFlag = uint8(1) << iota
	ForeignKeyFlag
	NotNullFlag
	UseZeroFlag
	UniqueFlag
	ArrayFlag
	customTypeFlag
)

type Field struct {
	Field reflect.StructField
	Type  reflect.Type
	Index []int

	GoName   string  // struct field name, e.g. Id
	SQLName  string  // SQL name, .e.g. id
	Column   types.Q // escaped SQL name, e.g. "id"
	SQLType  string
	Default  types.Q
	OnDelete string
	OnUpdate string

	flags uint8

	append types.AppenderFunc
	scan   types.ScannerFunc

	isZero iszero.Func
}

func indexEqual(ind1, ind2 []int) bool {
	if len(ind1) != len(ind2) {
		return false
	}
	for i, ind := range ind1 {
		if ind != ind2[i] {
			return false
		}
	}
	return true
}

func (f *Field) Clone() *Field {
	cp := *f
	cp.Index = cp.Index[:len(f.Index):len(f.Index)]
	return &cp
}

func (f *Field) SetFlag(flag uint8) {
	f.flags |= flag
}

func (f *Field) HasFlag(flag uint8) bool {
	return f.flags&flag != 0
}

func (f *Field) Value(strct reflect.Value) reflect.Value {
	return fieldByIndex(strct, f.Index)
}

func (f *Field) HasZeroValue(strct reflect.Value) bool {
	return f.hasZeroField(strct, f.Index)
}

func (f *Field) hasZeroField(v reflect.Value, index []int) bool {
	for _, idx := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return true
			}
			v = v.Elem()
		}
		v = v.Field(idx)
	}
	return f.isZero(v)
}

func (f *Field) NullZero() bool {
	return !f.HasFlag(UseZeroFlag)
}

func (f *Field) AppendValue(b []byte, strct reflect.Value, quote int) []byte {
	fv := f.Value(strct)
	if f.NullZero() && f.isZero(fv) {
		return types.AppendNull(b, quote)
	}
	if f.append == nil {
		panic(fmt.Errorf("pg: AppendValue(unsupported %s)", fv.Type()))
	}
	return f.append(b, fv, quote)
}

func (f *Field) ScanValue(strct reflect.Value, rd types.Reader, n int) error {
	fv := fieldByIndex(strct, f.Index)
	if f.scan == nil {
		return fmt.Errorf("pg: ScanValue(unsupported %s)", fv.Type())
	}
	return f.scan(fv, rd, n)
}

type Method struct {
	Index int

	flags int8

	appender func([]byte, reflect.Value, int) []byte
}

func (m *Method) Has(flag int8) bool {
	return m.flags&flag != 0
}

func (m *Method) Value(strct reflect.Value) reflect.Value {
	return strct.Method(m.Index).Call(nil)[0]
}

func (m *Method) AppendValue(dst []byte, strct reflect.Value, quote int) []byte {
	mv := m.Value(strct)
	return m.appender(dst, mv, quote)
}
