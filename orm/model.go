package orm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/pg.v3/types"
)

var invalidValue = reflect.Value{}

type Model struct {
	Joins []*Join

	Table *Table
	Path  []string
	bind  reflect.Value

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

func NewModelPath(bind reflect.Value, path []string, table *Table) (*Model, error) {
	return &Model{
		bind: bind,
		Path: path,

		Table: table,
	}, nil
}

func (m *Model) AppendPKName(b []byte) []byte {
	return types.AppendField(b, m.Table.Name+"."+m.Table.PK.SQLName, true)
}

func (m *Model) AppendPKValue(b []byte) []byte {
	return m.Table.PK.AppendValue(b, m.strct, true)
}

func (m *Model) AppendParam(b []byte, name string) ([]byte, error) {
	switch name {
	case "TableName":
		return types.AppendField(b, m.Table.Name, true), nil
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

func (s *Model) getJoin(name string) (*Join, bool) {
	for _, join := range s.Joins {
		if join.Relation.Field.SQLName == name {
			return join, true
		}
	}
	return nil, false
}

func (s *Model) Join(name string) error {
	path := strings.Split(name, ".")
	var goPath []string

	join := &Join{
		BaseModel: s,
		JoinModel: s,
	}
	var retErr error

	for _, name := range path {
		rel, ok := join.JoinModel.Table.Relations[name]
		if !ok {
			retErr = fmt.Errorf("pg: %s doesn't have %s relation", join.BaseModel.Table.Name, name)
			break
		}
		join.Relation = rel

		goPath = append(goPath, rel.Field.GoName)

		if v, ok := s.getJoin(name); ok {
			join.BaseModel = v.BaseModel
			join.JoinModel = v.JoinModel
			continue
		}

		model, err := NewModelPath(s.Value(), goPath, rel.Join)
		if err != nil {
			retErr = err
			break
		}

		join.BaseModel = join.JoinModel
		join.JoinModel = model
	}

	if join.JoinModel == join.BaseModel {
		return retErr
	}

	if v, ok := s.getJoin(join.Relation.Field.SQLName); ok {
		join = v
	} else {
		s.Joins = append(s.Joins, join)
	}

	switch len(path) - len(goPath) {
	case 0:
		// ok
	default:
		join.Columns = append(join.Columns, path[len(path)-1])
	}

	return nil
}

func (m *Model) NextModel() interface{} {
	v := m.Slice(true)
	if v.Kind() == reflect.Slice {
		m.strct = sliceNextElemValue(v)
	}

	for _, join := range m.Joins {
		if !join.Relation.Many {
			join.JoinModel.Bind(m.strct)
		}
	}

	return m
}

func (m *Model) ScanColumn(colIdx int, colName string, b []byte) error {
	modelName, fieldName := splitColumn(colName)
	join, ok := m.getJoin(modelName)
	if ok {
		return join.JoinModel.ScanColumn(colIdx, fieldName, b)
	}

	field, ok := m.Table.FieldsMap[colName]
	if !ok {
		return fmt.Errorf("pg: can't find field %q in %s", colName, m.strct.Type())
	}
	return field.DecodeValue(m.Struct(true), b)
}

func splitColumn(s string) (string, string) {
	parts := strings.SplitN(s, "__", 2)
	if len(parts) != 2 {
		return "", s
	}
	return parts[0], parts[1]
}
