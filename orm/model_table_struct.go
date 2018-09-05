package orm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type structTableModel struct {
	table *Table
	rel   *Relation
	joins []join

	root  reflect.Value
	index []int

	strct         reflect.Value
	structInited  bool
	structInitErr error
}

var _ tableModel = (*structTableModel)(nil)

func newStructTableModelValue(v reflect.Value) *structTableModel {
	return &structTableModel{
		table: GetTable(v.Type()),
		root:  v,
		strct: v,
	}
}

func newStructTableModelType(typ reflect.Type) *structTableModel {
	return &structTableModel{
		table: GetTable(typ),
	}
}

func (structTableModel) useQueryOne() bool {
	return true
}

func (m *structTableModel) Table() *Table {
	return m.table
}

func (m *structTableModel) Relation() *Relation {
	return m.rel
}

func (m *structTableModel) AppendParam(b []byte, f QueryFormatter, name string) ([]byte, bool) {
	b, ok := m.table.AppendParam(b, m.strct, name)
	if ok {
		return b, true
	}

	switch name {
	case "TableName":
		b = f.FormatQuery(b, string(m.table.Name))
		return b, true
	case "TableAlias":
		b = append(b, m.table.Alias...)
		return b, true
	case "TableColumns":
		b = appendColumns(b, m.table.Alias, m.table.Fields)
		return b, true
	case "Columns":
		b = appendColumns(b, "", m.table.Fields)
		return b, true
	}

	return b, false
}

func (m *structTableModel) Root() reflect.Value {
	return m.root
}

func (m *structTableModel) Index() []int {
	return m.index
}

func (m *structTableModel) ParentIndex() []int {
	return m.index[:len(m.index)-len(m.rel.Field.Index)]
}

func (m *structTableModel) Kind() reflect.Kind {
	return reflect.Struct
}

func (m *structTableModel) Value() reflect.Value {
	return m.strct
}

func (m *structTableModel) Mount(host reflect.Value) {
	m.strct = host.FieldByIndex(m.rel.Field.Index)
	m.structInited = false
}

func (m *structTableModel) initStruct() error {
	if m.structInited {
		return m.structInitErr
	}
	m.structInited = true

	switch m.strct.Kind() {
	case reflect.Invalid:
		m.structInitErr = errors.New("pg: Model(nil)")
		return m.structInitErr
	case reflect.Interface:
		m.strct = m.strct.Elem()
	}

	if m.strct.Kind() == reflect.Ptr {
		if m.strct.IsNil() {
			m.strct.Set(reflect.New(m.strct.Type().Elem()))
			m.strct = m.strct.Elem()
		} else {
			m.strct = m.strct.Elem()
		}
	}

	m.mountJoins()

	return nil
}

func (m *structTableModel) mountJoins() {
	for i := range m.joins {
		j := &m.joins[i]
		switch j.Rel.Type {
		case HasOneRelation, BelongsToRelation:
			j.JoinModel.Mount(m.strct)
		}
	}
}

func (structTableModel) Init() error {
	return nil
}

func (m *structTableModel) NewModel() ColumnScanner {
	return m
}

func (m *structTableModel) AddModel(_ ColumnScanner) error {
	return nil
}

func (m *structTableModel) AfterQuery(db DB) error {
	if !m.table.HasFlag(AfterQueryHookFlag) {
		return nil
	}
	return callAfterQueryHook(m.strct.Addr(), db)
}

func (m *structTableModel) BeforeSelectQuery(db DB, q *Query) (*Query, error) {
	if !m.table.HasFlag(BeforeSelectQueryHookFlag) {
		return q, nil
	}
	return callBeforeSelectQueryHook(m.table.zeroStruct.Addr(), db, q)
}

func (m *structTableModel) AfterSelect(db DB) error {
	if !m.table.HasFlag(AfterSelectHookFlag) {
		return nil
	}
	return callAfterSelectHook(m.strct.Addr(), db)
}

func (m *structTableModel) BeforeInsert(db DB) error {
	if !m.table.HasFlag(BeforeInsertHookFlag) {
		return nil
	}
	return callBeforeInsertHook(m.strct.Addr(), db)
}

func (m *structTableModel) AfterInsert(db DB) error {
	if !m.table.HasFlag(AfterInsertHookFlag) {
		return nil
	}
	return callAfterInsertHook(m.strct.Addr(), db)
}

func (m *structTableModel) BeforeUpdate(db DB) error {
	if !m.table.HasFlag(BeforeUpdateHookFlag) {
		return nil
	}
	return callBeforeUpdateHook(m.strct.Addr(), db)
}

func (m *structTableModel) AfterUpdate(db DB) error {
	if !m.table.HasFlag(AfterUpdateHookFlag) {
		return nil
	}
	return callAfterUpdateHook(m.strct.Addr(), db)
}

func (m *structTableModel) BeforeDelete(db DB) error {
	if !m.table.HasFlag(BeforeDeleteHookFlag) {
		return nil
	}
	return callBeforeDeleteHook(m.strct.Addr(), db)
}

func (m *structTableModel) AfterDelete(db DB) error {
	if !m.table.HasFlag(AfterDeleteHookFlag) {
		return nil
	}
	return callAfterDeleteHook(m.strct.Addr(), db)
}

func (m *structTableModel) ScanColumn(colIdx int, colName string, b []byte) error {
	ok, err := m.scanColumn(colIdx, colName, b)
	if ok {
		return err
	}
	if m.table.HasFlag(discardUnknownColumns) {
		return nil
	}
	return fmt.Errorf("pg: can't find column=%s in %s (try discard_unknown_columns)",
		colName, m.table)
}

func (m *structTableModel) scanColumn(
	colIdx int, colName string, b []byte,
) (bool, error) {
	// Don't init nil struct when value is NULL.
	if b == nil &&
		!m.structInited &&
		m.strct.Kind() == reflect.Ptr &&
		m.strct.IsNil() {
		return true, nil
	}

	err := m.initStruct()
	if err != nil {
		return true, err
	}

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
	if !ok {
		return false, nil
	}

	return true, field.ScanValue(m.strct, b)
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

func (m *structTableModel) Join(name string, apply func(*Query) (*Query, error)) *join {
	return m.join(m.Value(), name, apply)
}

func (m *structTableModel) join(
	bind reflect.Value, name string, apply func(*Query) (*Query, error),
) *join {
	path := strings.Split(name, ".")
	index := make([]int, 0, len(path))

	currJoin := join{
		BaseModel: m,
		JoinModel: m,
	}
	var lastJoin *join
	var hasColumnName bool

	for _, name := range path {
		rel, ok := currJoin.JoinModel.Table().Relations[name]
		if !ok {
			hasColumnName = true
			break
		}
		currJoin.Rel = rel
		index = append(index, rel.Field.Index...)

		if j := currJoin.JoinModel.GetJoin(name); j != nil {
			currJoin.BaseModel = j.BaseModel
			currJoin.JoinModel = j.JoinModel

			lastJoin = j
		} else {
			model, err := newTableModelIndex(bind, index, rel)
			if err != nil {
				return nil
			}

			currJoin.Parent = lastJoin
			currJoin.BaseModel = currJoin.JoinModel
			currJoin.JoinModel = model

			lastJoin = currJoin.BaseModel.AddJoin(currJoin)
		}
	}

	// No joins with such name.
	if lastJoin == nil {
		return nil
	}
	if apply != nil {
		lastJoin.ApplyQuery = apply
	}

	if hasColumnName {
		column := path[len(path)-1]
		if column == "_" {
			if lastJoin.Columns == nil {
				lastJoin.Columns = make([]string, 0)
			}
		} else {
			lastJoin.Columns = append(lastJoin.Columns, column)
		}
	}

	return lastJoin
}

func (m *structTableModel) setDeletedAt() {
	field := m.table.FieldsMap["deleted_at"]
	value := field.Value(m.strct)
	if value.Kind() == reflect.Ptr {
		now := time.Now()
		value.Set(reflect.ValueOf(&now))
	} else {
		value.Set(reflect.ValueOf(time.Now()))
	}
}

func splitColumn(s string) (string, string) {
	ind := strings.Index(s, "__")
	if ind == -1 {
		return "", s
	}
	return s[:ind], s[ind+2:]
}
