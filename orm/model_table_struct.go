package orm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type structTableModel struct {
	table *Table
	joins []join

	root  reflect.Value
	index []int

	strct reflect.Value
}

var _ tableModel = (*structTableModel)(nil)

func newStructTableModel(v interface{}) (*structTableModel, error) {
	switch v := v.(type) {
	case *structTableModel:
		return v, nil
	case reflect.Value:
		return newStructTableModelValue(v)
	default:
		return newStructTableModelValue(reflect.ValueOf(v))
	}
}

func newStructTableModelValue(v reflect.Value) (*structTableModel, error) {
	if !v.IsValid() {
		return nil, errors.New("pg: Model(nil)")
	}
	v = reflect.Indirect(v)

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("pg: Model(unsupported %s)", v.Type())
	}

	return &structTableModel{
		table: Tables.Get(v.Type()),
		root:  v,
		strct: v,
	}, nil
}

func (structTableModel) useQueryOne() bool {
	return true
}

func (m *structTableModel) Table() *Table {
	return m.table
}

func (m *structTableModel) AppendParam(dst []byte, name string) ([]byte, bool) {
	if field, ok := m.table.FieldsMap[name]; ok {
		dst = field.AppendValue(dst, m.strct, 1)
		return dst, true
	}

	if method, ok := m.table.Methods[name]; ok {
		dst = method.AppendValue(dst, m.strct.Addr(), 1)
		return dst, true
	}

	switch name {
	case "TableAlias":
		dst = append(dst, m.table.Alias...)
		return dst, true
	}

	return dst, false
}

func (m *structTableModel) Root() reflect.Value {
	return m.root
}

func (m *structTableModel) Index() []int {
	return m.index
}

func (m *structTableModel) Bind(bind reflect.Value) {
	m.strct = bind.Field(m.index[len(m.index)-1])
}

func (m *structTableModel) Value() reflect.Value {
	return m.strct
}

func (m *structTableModel) NewModel() ColumnScanner {
	for i := range m.joins {
		j := &m.joins[i]
		if j.Rel.One {
			j.JoinModel.Bind(m.strct)
		}
	}
	return m
}

func (structTableModel) AddModel(_ ColumnScanner) error {
	return nil
}

func (m *structTableModel) ScanColumn(colIdx int, colName string, b []byte) error {
	ok, err := m.scanColumn(colIdx, colName, b)
	if ok {
		return err
	}
	return fmt.Errorf("pg: can't find column %q in model %s", colName, m.table.ModelName)
}

func (m *structTableModel) scanColumn(colIdx int, colName string, b []byte) (bool, error) {
	joinName, fieldName := splitColumn(colName)
	if joinName != "" {
		if join := m.GetJoin(joinName); join != nil {
			return join.JoinModel.scanColumn(colIdx, fieldName, b)
		}
		if m.table.ModelName == joinName {
			return m.scanColumn(colIdx, fieldName, b)
		}
	}

	field, ok := m.table.FieldsMap[colName]
	if ok {
		if m.strct.Kind() == reflect.Interface {
			m.strct = m.strct.Elem()
		}
		m.strct = indirectNew(m.strct, true)
		return true, field.ScanValue(m.strct, b)
	}

	return false, nil
}

func (m *structTableModel) GetJoin(name string) *join {
	for i := range m.joins {
		j := &m.joins[i]
		if j.Rel.Field.GoName == name || j.Rel.Field.SQLName == name {
			return j
		}
	}
	return nil
}

func (m *structTableModel) GetJoins() []join {
	return m.joins
}

func (m *structTableModel) AddJoin(j join) *join {
	m.joins = append(m.joins, j)
	return &m.joins[len(m.joins)-1]
}

func (m *structTableModel) Join(name string) *join {
	return addJoin(m, m.Value(), name)
}

func addJoin(m *structTableModel, bind reflect.Value, name string) *join {
	path := strings.Split(name, ".")
	index := make([]int, 0, len(path))

	thejoin := join{
		BaseModel: m,
		JoinModel: m,
	}
	var lastJoin *join
	var hasColumnName bool

	for _, name := range path {
		rel, ok := thejoin.JoinModel.Table().Relations[name]
		if !ok {
			hasColumnName = true
			break
		}
		thejoin.Rel = rel
		index = append(index, rel.Field.Index...)

		if j := thejoin.JoinModel.GetJoin(name); j != nil {
			thejoin.BaseModel = j.BaseModel
			thejoin.JoinModel = j.JoinModel
			lastJoin = j
		} else {
			model, err := newTableModelIndex(bind, index, rel.Join)
			if err != nil {
				return nil
			}

			thejoin.BaseModel = thejoin.JoinModel
			thejoin.JoinModel = model
			lastJoin = thejoin.BaseModel.AddJoin(thejoin)
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
