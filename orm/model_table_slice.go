package orm

import (
	"context"
	"reflect"
	"time"

	"github.com/go-pg/pg/types"
)

type sliceTableModel struct {
	structTableModel

	slice      reflect.Value
	sliceLen   int
	sliceOfPtr bool
}

var _ TableModel = (*sliceTableModel)(nil)

func newSliceTableModel(slice reflect.Value, elemType reflect.Type) *sliceTableModel {
	m := &sliceTableModel{
		structTableModel: structTableModel{
			table: GetTable(elemType),
			root:  slice,
		},
		slice:    slice,
		sliceLen: slice.Len(),
	}
	m.init(slice.Type())
	return m
}

func (m *sliceTableModel) init(sliceType reflect.Type) {
	switch sliceType.Elem().Kind() {
	case reflect.Ptr, reflect.Interface:
		m.sliceOfPtr = true
	}
}

//nolint
func (*sliceTableModel) useQueryOne() {}

func (m *sliceTableModel) IsNil() bool {
	return false
}

func (m *sliceTableModel) AppendParam(fmter QueryFormatter, b []byte, name string) ([]byte, bool) {
	if field, ok := m.table.FieldsMap[name]; ok {
		b = append(b, "_data."...)
		b = append(b, field.Column...)
		return b, true
	}
	return m.structTableModel.AppendParam(fmter, b, name)
}

func (m *sliceTableModel) Join(name string, apply func(*Query) (*Query, error)) *join {
	return m.join(m.Value(), name, apply)
}

func (m *sliceTableModel) Bind(bind reflect.Value) {
	m.slice = bind.Field(m.index[len(m.index)-1])
}

func (m *sliceTableModel) Kind() reflect.Kind {
	return reflect.Slice
}

func (m *sliceTableModel) Value() reflect.Value {
	return m.slice
}

func (m *sliceTableModel) Init() error {
	if m.slice.IsValid() && m.slice.Len() > 0 {
		m.slice.Set(m.slice.Slice(0, 0))
	}
	return nil
}

func (m *sliceTableModel) NewModel() ColumnScanner {
	m.strct = m.nextElem()
	m.structInited = false
	return m
}

func (m *sliceTableModel) BeforeQuery(ctx context.Context, db DB) error {
	if m.table.HasFlag(BeforeQueryHookFlag) {
		return callBeforeQueryHookSlice(ctx, m.slice, m.sliceOfPtr, db)
	}
	return nil
}

func (m *sliceTableModel) AfterQuery(ctx context.Context, db DB) error {
	if m.table.HasFlag(AfterQueryHookFlag) {
		return callAfterQueryHookSlice(ctx, m.slice, m.sliceOfPtr, db)
	}
	return nil
}

func (m *sliceTableModel) AfterSelect(ctx context.Context, db DB) error {
	if m.table.HasFlag(AfterSelectHookFlag) {
		return callAfterSelectHookSlice(ctx, m.slice, m.sliceOfPtr, db)
	}
	return nil
}

func (m *sliceTableModel) BeforeInsert(ctx context.Context, db DB) error {
	if m.table.HasFlag(BeforeInsertHookFlag) {
		return callBeforeInsertHookSlice(ctx, m.slice, m.sliceOfPtr, db)
	}
	return nil
}

func (m *sliceTableModel) AfterInsert(ctx context.Context, db DB) error {
	if m.table.HasFlag(AfterInsertHookFlag) {
		return callAfterInsertHookSlice(ctx, m.slice, m.sliceOfPtr, db)
	}
	return nil
}

func (m *sliceTableModel) BeforeUpdate(ctx context.Context, db DB) error {
	if m.table.HasFlag(BeforeUpdateHookFlag) {
		return callBeforeUpdateHookSlice(ctx, m.slice, m.sliceOfPtr, db)
	}
	return nil
}

func (m *sliceTableModel) AfterUpdate(ctx context.Context, db DB) error {
	if m.table.HasFlag(AfterUpdateHookFlag) {
		return callAfterUpdateHookSlice(ctx, m.slice, m.sliceOfPtr, db)
	}
	return nil
}

func (m *sliceTableModel) BeforeDelete(ctx context.Context, db DB) error {
	if m.table.HasFlag(BeforeDeleteHookFlag) {
		return callBeforeDeleteHookSlice(ctx, m.slice, m.sliceOfPtr, db)
	}
	return nil
}

func (m *sliceTableModel) AfterDelete(ctx context.Context, db DB) error {
	if m.table.HasFlag(AfterDeleteHookFlag) {
		return callAfterDeleteHookSlice(ctx, m.slice, m.sliceOfPtr, db)
	}
	return nil
}

func (m *sliceTableModel) nextElem() reflect.Value {
	if m.slice.Len() < m.slice.Cap() {
		m.slice.Set(m.slice.Slice(0, m.slice.Len()+1))
		elem := m.slice.Index(m.slice.Len() - 1)
		if m.sliceOfPtr {
			if elem.IsNil() {
				elem.Set(reflect.New(elem.Type().Elem()))
			}
			return elem.Elem()
		}
		return elem
	}

	if m.sliceOfPtr {
		elem := reflect.New(m.table.Type)
		m.slice.Set(reflect.Append(m.slice, elem))
		return elem.Elem()
	}

	m.slice.Set(reflect.Append(m.slice, m.table.zeroStruct))
	return m.slice.Index(m.slice.Len() - 1)
}

func (m *sliceTableModel) setSoftDeleteField() {
	field := m.table.SoftDeleteField
	now := time.Now()
	var value reflect.Value

	switch {
	case m.sliceOfPtr:
		value = reflect.ValueOf(&now)
	case field.Type == timeType:
		value = reflect.ValueOf(now)
	default:
		value = reflect.ValueOf(types.NullTime{Time: now})
	}

	for i := 0; i < m.slice.Len(); i++ {
		strct := indirect(m.slice.Index(i))
		field.Value(strct).Set(value)
	}
}
