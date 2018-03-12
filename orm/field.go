package orm

import (
	"reflect"

	"github.com/go-pg/pg/types"
)

const (
	PrimaryKeyFlag = uint8(1) << iota
	ForeignKeyFlag
	NotNullFlag
	UniqueFlag
	ArrayFlag
	customTypeFlag
)

type Field struct {
	Type reflect.Type

	GoName   string  // struct field name, e.g. Id
	GoName_  string  // underscored struct field name, e.g. id
	SQLName  string  // SQL name, .e.g. id
	Column   types.Q // escaped SQL name, e.g. "id"
	SQLType  string
	Index    []int
	Default  types.Q
	OnDelete string

	flags uint8

	append types.AppenderFunc
	scan   types.ScannerFunc

	isZero func(reflect.Value) bool
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

func (f *Field) Copy() *Field {
	copy := *f
	copy.Index = copy.Index[:len(f.Index):len(f.Index)]
	return &copy
}

func (f *Field) SetFlag(flag uint8) {
	f.flags |= flag
}

func (f *Field) HasFlag(flag uint8) bool {
	return f.flags&flag != 0
}

func (f *Field) Value(strct reflect.Value) reflect.Value {
	return strct.FieldByIndex(f.Index)
}

func (f *Field) IsZero(strct reflect.Value) bool {
	fv := f.Value(strct)
	return f.isZero(fv)
}

func (f *Field) OmitZero(strct reflect.Value) bool {
	return (f.Default != "" || !f.HasFlag(NotNullFlag)) && f.isZero(f.Value(strct))
}

func (f *Field) AppendValue(b []byte, strct reflect.Value, quote int) []byte {
	fv := f.Value(strct)
	if !f.HasFlag(NotNullFlag) && f.isZero(fv) {
		return types.AppendNull(b, quote)
	}
	return f.append(b, fv, quote)
}

func (f *Field) ScanValue(strct reflect.Value, b []byte) error {
	fv := fieldByIndex(strct, f.Index)
	return f.scan(fv, b)
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
