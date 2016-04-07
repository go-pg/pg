package orm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// TODO: extract AppendParam to separate struct and use it in Formatter
type StructModel struct {
	table *Table
	joins []Join

	root reflect.Value
	path []string

	strct reflect.Value
}

var _ TableModel = (*StructModel)(nil)

func NewStructModel(v interface{}) (*StructModel, error) {
	switch v := v.(type) {
	case *StructModel:
		return v, nil
	case reflect.Value:
		return newStructModelValue(v)
	default:
		return newStructModelValue(reflect.ValueOf(v))
	}
}

func newStructModelValue(v reflect.Value) (*StructModel, error) {
	if !v.IsValid() {
		return nil, errors.New("pg: NewStructModel(nil)")
	}
	v = reflect.Indirect(v)

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("pg: NewStructModel(unsupported %s)", v.Type())
	}

	return &StructModel{
		table: Tables.Get(v.Type()),
		root:  v,
		strct: v,
	}, nil
}

func (m *StructModel) Table() *Table {
	return m.table
}

func (m *StructModel) AppendParam(dst []byte, name string) ([]byte, bool) {
	if field, ok := m.table.FieldsMap[name]; ok {
		dst = field.AppendValue(dst, m.strct, 1)
		return dst, true
	}

	if method, ok := m.table.Methods[name]; ok {
		dst = method.AppendValue(dst, m.strct.Addr(), 1)
		return dst, true
	}

	return dst, false
}

func (m *StructModel) Kind() reflect.Kind {
	return reflect.Struct
}

func (m *StructModel) Root() reflect.Value {
	return m.root
}

func (m *StructModel) Path() []string {
	return m.path
}

func (m *StructModel) Bind(bind reflect.Value) {
	m.strct = bind.FieldByName(m.path[len(m.path)-1])
}

func (m *StructModel) Value() reflect.Value {
	return m.strct
}

func (m *StructModel) NewModel() ColumnScanner {
	for i := range m.joins {
		j := &m.joins[i]
		if j.Rel.One {
			j.JoinModel.Bind(m.strct)
		}
	}
	return m
}

func (StructModel) AddModel(_ ColumnScanner) error {
	return nil
}

func (m *StructModel) ScanColumn(colIdx int, colName string, b []byte) error {
	ok, err := m.scanColumn(colIdx, colName, b)
	if ok {
		return err
	}
	return fmt.Errorf("pg: can't find column %q in model %s", colName, m.table.ModelName)
}

func (m *StructModel) scanColumn(colIdx int, colName string, b []byte) (bool, error) {
	field, ok := m.table.FieldsMap[colName]
	if ok {
		return true, field.ScanValue(m.strct, b)
	}

	joinName, fieldName := splitColumn(colName)
	if joinName != "" {
		if join := m.GetJoin(joinName); join != nil {
			return join.JoinModel.scanColumn(colIdx, fieldName, b)
		}
	}

	field, ok = m.table.FieldsMap[fieldName]
	if ok {
		if m.strct.Kind() == reflect.Ptr {
			if m.strct.IsNil() {
				m.strct.Set(reflect.New(m.strct.Type().Elem()))
			}
			m.strct = m.strct.Elem()
		}
		return true, field.ScanValue(m.strct, b)
	}

	return false, nil
}

func (m *StructModel) GetJoin(name string) *Join {
	for i := range m.joins {
		j := &m.joins[i]
		if j.Rel.Field.GoName == name || j.Rel.Field.SQLName == name {
			return j
		}
	}
	return nil
}

func (m *StructModel) GetJoins() []Join {
	return m.joins
}

func (m *StructModel) AddJoin(j Join) *Join {
	m.joins = append(m.joins, j)
	return &m.joins[len(m.joins)-1]
}

func (m *StructModel) Join(name string) *Join {
	return join(m, m.Value(), name)
}

func join(m *StructModel, bind reflect.Value, name string) *Join {
	path := strings.Split(name, ".")

	join := Join{
		BaseModel: m,
		JoinModel: m,
	}
	var lastJoin *Join
	var hasColumnName bool

	for i, name := range path {
		rel, ok := join.JoinModel.Table().Relations[name]
		if !ok {
			hasColumnName = true
			break
		}
		join.Rel = rel

		if j := join.JoinModel.GetJoin(name); j != nil {
			join.BaseModel = j.BaseModel
			join.JoinModel = j.JoinModel
			lastJoin = j
		} else {
			model, err := NewTableModelPath(bind, path[:i+1], rel.Join)
			if err != nil {
				return nil
			}

			join.BaseModel = join.JoinModel
			join.JoinModel = model
			lastJoin = join.BaseModel.AddJoin(join)
		}
	}

	// No joins with such name.
	if lastJoin == nil {
		return nil
	}

	if hasColumnName {
		column := path[len(path)-1]
		if column == "_" {
			column = path[len(path)-2]
		} else {
			lastJoin.Columns = append(lastJoin.Columns, column)
		}
	} else {
		lastJoin.SelectAll = true
	}

	return lastJoin
}

func splitColumn(s string) (string, string) {
	ind := strings.Index(s, "__")
	if ind == -1 {
		return "", s
	}
	return s[:ind], s[ind+2:]
}
