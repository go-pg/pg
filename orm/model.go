package orm

import (
	"errors"
	"fmt"
	"reflect"

	"gopkg.in/pg.v3/types"
)

var invalidValue = reflect.Value{}

type Model struct {
	Table   *Table
	Columns []string
	Path    []string
	bind    reflect.Value

	strct reflect.Value
	slice reflect.Value
}

var (
	_ Collection    = (*Model)(nil)
	_ ColumnScanner = (*Model)(nil)
)

func NewModel(vi interface{}) (*Model, error) {
	v, ok := vi.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(vi)
	}
	if !v.IsValid() {
		return nil, errors.New("pg: NewModel(nil)")
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		return &Model{
			strct: v,

			Table: Tables.Get(v.Type()),
		}, nil
	case reflect.Slice:
		typ := indirectType(v.Type().Elem())
		return &Model{
			slice: v,

			Table: Tables.Get(typ),
		}, nil
	default:
		return nil, fmt.Errorf("pg: NewModel(unsupported %T)", vi)
	}
}

func NewModelPath(bind reflect.Value, path []string) (*Model, error) {
	typ := fieldValueByPath(bind, path, false).Type()
	switch typ.Kind() {
	case reflect.Struct:
		return &Model{
			bind: bind,
			Path: path,

			Table: Tables.Get(typ),
		}, nil
	case reflect.Slice:
		typ := indirectType(typ.Elem())
		return &Model{
			bind: bind,
			Path: path,

			Table: Tables.Get(typ),
		}, nil
	default:
		return nil, fmt.Errorf("pg: NewModelPath(unsupported %s)", typ)
	}
}

func (m *Model) AppendPKName(b []byte) []byte {
	return types.AppendField(b, m.Table.Name+"."+m.Table.PK.SQLName)
}

func (m *Model) AppendPKValue(b []byte) []byte {
	return m.Table.PK.AppendValue(b, m.strct, true)
}

func (m *Model) AppendColumns(columns []string, prefix string) []string {
	if m.Columns != nil {
		for _, column := range m.Columns {
			column = fmt.Sprintf("%s.%s AS %s", m.Table.Name, column, prefix+column)
			columns = append(columns, column)
		}
		return columns
	}
	for _, f := range m.Table.Fields {
		column := fmt.Sprintf("%s.%s AS %s", m.Table.Name, f.SQLName, prefix+f.SQLName)
		columns = append(columns, column)
	}
	return columns
}

func (m *Model) AppendParam(b []byte, name string) ([]byte, error) {
	switch name {
	case "TableName":
		return types.AppendField(b, m.Table.Name), nil
	case "PK":
		return m.AppendPKName(b), nil
	case "PKValue":
		return m.AppendPKValue(b), nil
	}

	if field, ok := m.Table.FieldsMap[name]; ok {
		return field.AppendValue(b, m.strct, true), nil
	}

	if method, ok := m.Table.Methods[name]; ok {
		return method.AppendValue(b, m.strct.Addr(), true), nil
	}

	return nil, fmt.Errorf("pg: can't map %q on %s", name, m.strct.Type())
}

func (m *Model) NextModel() interface{} {
	v := m.Slice(true)
	if v.Kind() == reflect.Slice {
		m.strct = sliceNextElemValue(v)
	}
	return m
}

func (m *Model) ScanColumn(colIdx int, colName string, b []byte) error {
	field, ok := m.Table.FieldsMap[colName]
	if !ok {
		return fmt.Errorf("pg: can't find field %q in %s", colName, m.strct.Type())
	}
	return field.DecodeValue(m.Struct(true), b)
}

func (m *Model) Bind(bind reflect.Value) {
	m.bind = bind
	m.strct = invalidValue
	m.slice = invalidValue
}

func (m *Model) Value() reflect.Value {
	if m.slice.IsValid() {
		return m.slice
	}
	if m.strct.IsValid() {
		return m.strct
	}
	return fieldValueByPath(m.bind, m.Path, false)
}

func (m *Model) Struct(save bool) reflect.Value {
	if m.strct.IsValid() {
		return m.strct
	}
	v := fieldValueByPath(m.bind, m.Path, save)
	if save {
		m.strct = v
	}
	return v
}

func (m *Model) Slice(save bool) reflect.Value {
	if m.slice.IsValid() {
		return m.slice
	}
	v := fieldValueByPath(m.bind, m.Path, save)
	if save {
		m.slice = v
	}
	return v
}

func fieldValueByPath(v reflect.Value, path []string, save bool) reflect.Value {
	for _, name := range path {
		if v.Kind() == reflect.Slice {
			v = reflect.Zero(v.Type().Elem())
		}

		v = v.FieldByName(name)
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				if save {
					v.Set(reflect.New(v.Type().Elem()))
				} else {
					v = reflect.New(v.Type().Elem())
				}
			}
			v = v.Elem()
		}
	}
	return v
}

func sliceNextElemValue(v reflect.Value) reflect.Value {
	switch v.Type().Elem().Kind() {
	case reflect.Ptr:
		elem := reflect.New(v.Type().Elem().Elem())
		v.Set(reflect.Append(v, elem))
		return elem.Elem()
	case reflect.Struct:
		elem := reflect.New(v.Type().Elem()).Elem()
		v.Set(reflect.Append(v, elem))
		elem = v.Index(v.Len() - 1)
		return elem
	default:
		panic("not reached")
	}
}
