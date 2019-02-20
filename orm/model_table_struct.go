package orm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-pg/pg/types"
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

var _ TableModel = (*structTableModel)(nil)

func newStructTableModel(table *Table) *structTableModel {
	return &structTableModel{
		table: table,
	}
}

func newStructTableModelValue(v reflect.Value) *structTableModel {
	return &structTableModel{
		table: GetTable(v.Type()),
		root:  v,
		strct: v,
	}
}

func (*structTableModel) useQueryOne() bool {
	return true
}

func (m *structTableModel) IsNil() bool {
	return !m.strct.IsValid()
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
		b = f.FormatQuery(b, string(m.table.FullName))
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
		m.structInitErr = errModelNil
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

func (m *structTableModel) AfterQuery(c context.Context, db DB) error {
	if !m.table.HasFlag(AfterQueryHookFlag) {
		return nil
	}
	return callAfterQueryHook(m.strct.Addr(), c, db)
}

func (m *structTableModel) BeforeSelectQuery(c context.Context, db DB, q *Query) (*Query, error) {
	if m.table.HasFlag(BeforeSelectQueryHookFlag) {
		return callBeforeSelectQueryHook(m.table.zeroStruct.Addr(), c, db, q)
	}
	return q, nil
}

func (m *structTableModel) AfterSelect(c context.Context, db DB) error {
	if m.table.HasFlag(AfterSelectHookFlag) {
		if m.IsNil() {
			return errModelNil
		}
		return callAfterSelectHook(m.strct.Addr(), c, db)
	}
	return nil
}

func (m *structTableModel) BeforeInsert(c context.Context, db DB) error {
	if m.table.HasFlag(BeforeInsertHookFlag) {
		if m.IsNil() {
			return errModelNil
		}
		return callBeforeInsertHook(m.strct.Addr(), c, db)
	}
	return nil
}

func (m *structTableModel) AfterInsert(c context.Context, db DB) error {
	if m.table.HasFlag(AfterInsertHookFlag) {
		if m.IsNil() {
			return errModelNil
		}
		return callAfterInsertHook(m.strct.Addr(), c, db)
	}
	return nil
}

func (m *structTableModel) BeforeUpdate(c context.Context, db DB) error {
	if m.table.HasFlag(BeforeUpdateHookFlag) && !m.IsNil() {
		return callBeforeUpdateHook(m.strct.Addr(), c, db)
	}
	return nil
}

func (m *structTableModel) AfterUpdate(c context.Context, db DB) error {
	if m.table.HasFlag(AfterUpdateHookFlag) && !m.IsNil() {
		return callAfterUpdateHook(m.strct.Addr(), c, db)
	}
	return nil
}

func (m *structTableModel) BeforeDelete(c context.Context, db DB) error {
	if m.table.HasFlag(BeforeDeleteHookFlag) && !m.IsNil() {
		return callBeforeDeleteHook(m.strct.Addr(), c, db)
	}
	return nil
}

func (m *structTableModel) AfterDelete(c context.Context, db DB) error {
	if m.table.HasFlag(AfterDeleteHookFlag) && !m.IsNil() {
		return callAfterDeleteHook(m.strct.Addr(), c, db)
	}
	return nil
}

func (m *structTableModel) ScanColumn(
	colIdx int, colName string, rd types.Reader, n int,
) error {
	ok, err := m.scanColumn(colIdx, colName, rd, n)
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
	colIdx int, colName string, rd types.Reader, n int,
) (bool, error) {
	// Don't init nil struct when value is NULL.
	if n == -1 &&
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
			return join.JoinModel.scanColumn(colIdx, fieldName, rd, n)
		}
		if m.table.ModelName == joinName {
			return m.scanColumn(colIdx, fieldName, rd, n)
		}
	}

	field, ok := m.table.FieldsMap[colName]
	if !ok {
		return false, nil
	}

	return true, field.ScanValue(m.strct, rd, n)
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

func (m *structTableModel) setSoftDeleteField() {
	field := m.table.SoftDeleteField
	value := field.Value(m.strct)

	now := time.Now()
	if value.Kind() == reflect.Ptr {
		value.Set(reflect.ValueOf(&now))
	} else if field.Type == timeType {
		value.Set(reflect.ValueOf(now))
	} else {
		value.Set(reflect.ValueOf(types.NullTime{Time: now}))
	}
}

func splitColumn(s string) (string, string) {
	ind := strings.Index(s, "__")
	if ind == -1 {
		return "", s
	}
	return s[:ind], s[ind+2:]
}
