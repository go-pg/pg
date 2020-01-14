package orm

import (
	"context"
	"reflect"
	"time"

	"github.com/go-pg/pg/v9/internal"
	"github.com/go-pg/pg/v9/types"
)

type sliceTableModel struct {
	structTableModel

	slice      reflect.Value
	sliceLen   int
	sliceOfPtr bool
	nextElem   func() reflect.Value
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
		nextElem: internal.MakeSliceNextElemFunc(slice),
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

func (m *sliceTableModel) NextColumnScanner() ColumnScanner {
	m.strct = m.nextElem()
	m.structInited = false
	return m
}

func (m *sliceTableModel) AddColumnScanner(_ ColumnScanner) error {
	return nil
}

var _ BeforeScanHook = (*sliceTableModel)(nil)

func (m *sliceTableModel) BeforeScan(c context.Context) error {
	if m.table.hasFlag(beforeScanHookFlag) {
		return callBeforeScanHook(c, m.strct.Addr())
	}
	return nil
}

var _ AfterScanHook = (*sliceTableModel)(nil)

func (m *sliceTableModel) AfterScan(c context.Context) error {
	if m.table.hasFlag(afterScanHookFlag) {
		return callAfterScanHook(c, m.strct.Addr())
	}
	return nil
}

func (m *sliceTableModel) AfterSelect(c context.Context) error {
	if m.table.hasFlag(afterSelectHookFlag) {
		return callAfterSelectHookSlice(c, m.slice, m.sliceOfPtr)
	}
	return nil
}

func (m *sliceTableModel) BeforeInsert(c context.Context) (context.Context, error) {
	if m.table.hasFlag(beforeInsertHookFlag) {
		return callBeforeInsertHookSlice(c, m.slice, m.sliceOfPtr)
	}
	return c, nil
}

func (m *sliceTableModel) AfterInsert(c context.Context) error {
	if m.table.hasFlag(afterInsertHookFlag) {
		return callAfterInsertHookSlice(c, m.slice, m.sliceOfPtr)
	}
	return nil
}

func (m *sliceTableModel) BeforeUpdate(c context.Context) (context.Context, error) {
	if m.table.hasFlag(beforeUpdateHookFlag) && !m.IsNil() {
		return callBeforeUpdateHookSlice(c, m.slice, m.sliceOfPtr)
	}
	return c, nil
}

func (m *sliceTableModel) AfterUpdate(c context.Context) error {
	if m.table.hasFlag(afterUpdateHookFlag) {
		return callAfterUpdateHookSlice(c, m.slice, m.sliceOfPtr)
	}
	return nil
}

func (m *sliceTableModel) BeforeDelete(c context.Context) (context.Context, error) {
	if m.table.hasFlag(beforeDeleteHookFlag) && !m.IsNil() {
		return callBeforeDeleteHookSlice(c, m.slice, m.sliceOfPtr)
	}
	return c, nil
}

func (m *sliceTableModel) AfterDelete(c context.Context) error {
	if m.table.hasFlag(afterDeleteHookFlag) && !m.IsNil() {
		return callAfterDeleteHookSlice(c, m.slice, m.sliceOfPtr)
	}
	return nil
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
