package orm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/pg.v4/types"
)

var invalidValue = reflect.Value{}

type TableModel struct {
	Joins []Join

	Table *Table
	Path  []string
	bind  reflect.Value

	strct reflect.Value
	slice reflect.Value
}

var (
	_ Collection    = (*TableModel)(nil)
	_ ColumnScanner = (*TableModel)(nil)
)

func NewTableModel(v interface{}) (*TableModel, error) {
	switch v := (v).(type) {
	case *TableModel:
		return v, nil
	case reflect.Value:
		return newTableModelValue(v)
	default:
		return newTableModelValue(reflect.ValueOf(v))
	}
}

func newTableModelValue(v reflect.Value) (*TableModel, error) {
	if !v.IsValid() {
		return nil, errors.New("pg: NewModel(nil)")
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		return &TableModel{
			strct: v,

			Table: Tables.Get(v.Type()),
		}, nil
	case reflect.Slice:
		elType := indirectType(v.Type().Elem())
		if elType.Kind() == reflect.Struct {
			return &TableModel{
				slice: v,

				Table: Tables.Get(elType),
			}, nil
		}
	}

	return nil, fmt.Errorf("pg: NewModel(unsupported %s)", v.Type())
}

func NewTableModelPath(bind reflect.Value, path []string, table *Table) (*TableModel, error) {
	return &TableModel{
		bind: bind,
		Path: path,

		Table: table,
	}, nil
}

func (m *TableModel) AppendPK(b []byte) []byte {
	return columns("", m.Table.PKs)
}

func (m *TableModel) AppendPKValue(b []byte) []byte {
	for i, pk := range m.Table.PKs {
		b = pk.AppendValue(b, m.strct, true)
		if i != len(m.Table.PKs) {
			b = append(b, ", "...)
		}
	}
	return b
}

func (m *TableModel) AppendParam(b []byte, name string) ([]byte, error) {
	switch name {
	case "Table":
		return types.AppendField(b, m.Table.Name, true), nil
	case "PK":
		return m.AppendPK(b), nil
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

func (m *TableModel) Bind(bind reflect.Value) {
	m.bind = bind
	m.strct = invalidValue
	m.slice = invalidValue
}

func (m *TableModel) Kind() reflect.Kind {
	if m.slice.IsValid() {
		return m.slice.Kind()
	}
	if m.strct.IsValid() {
		return m.strct.Kind()
	}
	return fieldValueByPath(m.bind, m.Path, false).Kind()
}

func (m *TableModel) Value() reflect.Value {
	if m.slice.IsValid() {
		return m.slice
	}
	if m.strct.IsValid() {
		return m.strct
	}
	return fieldValueByPath(m.bind, m.Path, false)
}

func (m *TableModel) Struct(save bool) reflect.Value {
	if m.strct.IsValid() {
		return m.strct
	}
	v := fieldValueByPath(m.bind, m.Path, save)
	if save {
		m.strct = v
	}
	return v
}

func (m *TableModel) Slice(save bool) reflect.Value {
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

func (m *TableModel) getJoin(name string) (*Join, bool) {
	for i := range m.Joins {
		j := &m.Joins[i]
		if j.Rel.Field.GoName == name {
			return j, true
		}
	}
	return nil, false
}

func (m *TableModel) Join(name string) (string, error) {
	path := strings.Split(name, ".")
	var sqlPath []string

	join := Join{
		BaseModel: m,
		JoinModel: m,
	}
	var retErr error

	var start int
	for i, name := range path {
		rel, ok := join.JoinModel.Table.Relations[name]
		if !ok {
			retErr = fmt.Errorf(
				"pg: %s doesn't have %s relation",
				join.BaseModel.Table.ModelName, name,
			)
			break
		}

		join.Rel = rel
		sqlPath = append(sqlPath, rel.Field.SQLName)

		if j, ok := join.JoinModel.getJoin(name); ok {
			join.BaseModel = j.BaseModel
			join.JoinModel = j.JoinModel
		} else {
			model, err := NewTableModelPath(m.Value(), path[start:i+1], rel.Join)
			if err != nil {
				retErr = err
				break
			}

			join.BaseModel = join.JoinModel
			join.JoinModel = model
			join.BaseModel.Joins = append(join.BaseModel.Joins, join)
		}

		// Reset path on HasMany relation.
		if !rel.One {
			start = i + 1
		}
	}

	// No joins with such name.
	if join.JoinModel == join.BaseModel {
		return "", retErr
	}

	// Get reference to modify join in the Joins slice.
	lastJoin := &join.BaseModel.Joins[len(join.BaseModel.Joins)-1]

	if retErr == nil {
		lastJoin.SelectAll = true
	} else {
		column := path[len(path)-1]
		if column == "_" {
			column = path[len(path)-2]
		} else {
			lastJoin.Columns = append(lastJoin.Columns, column)
		}
		sqlPath = append(sqlPath, column)
	}

	return strings.Join(sqlPath, "."), nil
}

func (m *TableModel) NewModel() ColumnScanner {
	v := m.Slice(true)
	if v.Kind() != reflect.Slice {
		return m
	}
	m.strct = sliceNextElemValue(v)

	for i := range m.Joins {
		j := &m.Joins[i]
		if j.Rel.One {
			j.JoinModel.Bind(m.strct)
		}
	}

	return m
}

func (m *TableModel) ScanColumn(colIdx int, colName string, b []byte) error {
	joinName, fieldName := splitColumn(colName)
	join, ok := m.getJoin(joinName)
	if ok {
		return join.JoinModel.ScanColumn(colIdx, fieldName, b)
	}

	field, ok := m.Table.FieldsMap[colName]
	if !ok {
		return fmt.Errorf("pg: can't find field %q in %s", colName, m.strct.Type())
	}
	return field.DecodeValue(m.Struct(true), b)
}

const columnSep = "__"

func splitColumn(s string) (string, string) {
	if !strings.Contains(s, columnSep) {
		return "", s
	}
	parts := strings.SplitN(s, columnSep, 2)
	return parts[0], parts[1]
}
