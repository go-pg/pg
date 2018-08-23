package orm

import (
	"reflect"
	"time"
)

type sliceTableModel struct {
	structTableModel

	slice      reflect.Value
	sliceOfPtr bool
}

var _ tableModel = (*sliceTableModel)(nil)

func (m *sliceTableModel) init(sliceType reflect.Type) {
	switch sliceType.Elem().Kind() {
	case reflect.Ptr, reflect.Interface:
		m.sliceOfPtr = true
	}
}

func (sliceTableModel) useQueryOne() {}

func (m *sliceTableModel) AppendParam(b []byte, f QueryFormatter, name string) ([]byte, bool) {
	if field, ok := m.table.FieldsMap[name]; ok {
		b = append(b, "_data."...)
		b = append(b, field.Column...)
		return b, true
	}

	return m.structTableModel.AppendParam(b, f, name)
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

func (m *sliceTableModel) AfterQuery(db DB) error {
	if !m.table.HasFlag(AfterQueryHookFlag) {
		return nil
	}
	return callAfterQueryHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) AfterSelect(db DB) error {
	if !m.table.HasFlag(AfterSelectHookFlag) {
		return nil
	}
	return callAfterSelectHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) BeforeInsert(db DB) error {
	if !m.table.HasFlag(BeforeInsertHookFlag) {
		return nil
	}
	return callBeforeInsertHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) AfterInsert(db DB) error {
	if !m.table.HasFlag(AfterInsertHookFlag) {
		return nil
	}
	return callAfterInsertHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) BeforeUpdate(db DB) error {
	if !m.table.HasFlag(BeforeUpdateHookFlag) {
		return nil
	}
	return callBeforeUpdateHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) AfterUpdate(db DB) error {
	if !m.table.HasFlag(AfterUpdateHookFlag) {
		return nil
	}
	return callAfterUpdateHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) BeforeDelete(db DB) error {
	if !m.table.HasFlag(BeforeDeleteHookFlag) {
		return nil
	}
	return callBeforeDeleteHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) AfterDelete(db DB) error {
	if !m.table.HasFlag(AfterDeleteHookFlag) {
		return nil
	}
	return callAfterDeleteHookSlice(m.slice, m.sliceOfPtr, db)
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

func (m *sliceTableModel) setDeletedAt() {
	field := m.table.FieldsMap["deleted_at"]
	now := time.Now()
	for i := 0; i < m.slice.Len(); i++ {
		strct := indirect(m.slice.Index(i))
		value := field.Value(strct)
		value.Set(reflect.ValueOf(now))
	}
}
