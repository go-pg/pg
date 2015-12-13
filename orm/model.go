package orm

import (
	"errors"
	"fmt"
	"reflect"

	"gopkg.in/pg.v3/types"
)

var invalidValue = reflect.Value{}

type Model struct {
	value reflect.Value
	slice reflect.Value

	owner reflect.Value
	path  []string

	Table *Table
}

var (
	_ Collection    = (*Model)(nil)
	_ ColumnScanner = (*Model)(nil)
)

func NewModel(vi interface{}) (*Model, error) {
	v := reflect.ValueOf(vi)
	if !v.IsValid() {
		return nil, errors.New("pg: NewModel(nil)")
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		return &Model{
			value: v,

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

func NewModelPath(owner reflect.Value, path []string) (*Model, error) {
	typ := fieldValueByPath(owner, path, false).Type()
	switch typ.Kind() {
	case reflect.Struct:
		return &Model{
			owner: owner,
			path:  path,

			Table: Tables.Get(typ),
		}, nil
	case reflect.Slice:
		typ := indirectType(typ.Elem())
		return &Model{
			owner: owner,
			path:  path,

			Table: Tables.Get(typ),
		}, nil
	default:
		return nil, fmt.Errorf("pg: NewModelPath(unsupported %s)", typ)
	}
}

// TODO - append?
func (m *Model) PK() string {
	return m.Table.PK.SQLName
}

// TODO - append?
func (m *Model) PKValue() types.Q {
	return types.Q(m.Table.PK.AppendValue(nil, m.Value(false), false))
}

func (m *Model) Columns(columns []string, prefix string) []string {
	for _, f := range m.Table.Fields {
		column := fmt.Sprintf(`%s.%s AS %s`, m.Table.Name, f.SQLName, prefix+f.SQLName)
		columns = append(columns, column)
	}
	return columns
}

func (m *Model) AppendParam(b []byte, name string) ([]byte, error) {
	switch name {
	case "PK":
		return append(b, m.PK()...), nil
	}

	if field, ok := m.Table.FieldsMap[name]; ok {
		return field.AppendValue(b, m.value, true), nil
	}

	if method, ok := m.Table.Methods[name]; ok {
		return method.AppendValue(b, m.value.Addr(), true), nil
	}

	return nil, fmt.Errorf("pg: can't map %q on %s", name, m.value.Type())
}

func (m *Model) NextModel() interface{} {
	v := m.Slice(true)
	if v.Kind() == reflect.Slice {
		m.value = sliceNextElemValue(v)
	}
	return m
}

func (m *Model) ScanColumn(colIdx int, colName string, b []byte) error {
	field, ok := m.Table.FieldsMap[colName]
	if !ok {
		return fmt.Errorf("pg: can't find field %q in %s", colName, m.value.Type())
	}
	return field.DecodeValue(m.Value(true), b)
}

func (m *Model) Bind(owner reflect.Value) {
	m.owner = owner
	m.value = invalidValue
	m.slice = invalidValue
}

func (m *Model) Value(save bool) reflect.Value {
	if m.value.IsValid() {
		return m.value
	}
	v := fieldValueByPath(m.owner, m.path, save)
	if save {
		m.value = v
	}
	return v
}

func (m *Model) Slice(save bool) reflect.Value {
	if m.slice.IsValid() {
		return m.slice
	}
	v := fieldValueByPath(m.owner, m.path, save)
	if save {
		m.slice = v
	}
	return v
}

func fieldValueByPath(v reflect.Value, path []string, save bool) reflect.Value {
	for _, name := range path {
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
