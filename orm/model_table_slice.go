package orm

import "reflect"

type sliceTableModel struct {
	structTableModel

	slice      reflect.Value
	sliceOfPtr bool
	zeroElem   reflect.Value
}

var _ tableModel = (*sliceTableModel)(nil)

func (m *sliceTableModel) init(sliceType reflect.Type) {
	switch sliceType.Elem().Kind() {
	case reflect.Ptr, reflect.Interface:
		m.sliceOfPtr = true
	}
	if !m.sliceOfPtr {
		m.zeroElem = reflect.Zero(m.table.Type)
	}
}

func (sliceTableModel) useQueryOne() bool {
	return false
}

func (m *sliceTableModel) Join(name string, apply func(*Query) *Query) *join {
	return m.join(m.Value(), name, apply)
}

func (m *sliceTableModel) Bind(bind reflect.Value) {
	m.slice = bind.Field(m.index[len(m.index)-1])
}

func (m *sliceTableModel) Value() reflect.Value {
	return m.slice
}

func (m *sliceTableModel) NewModel() ColumnScanner {
	if !m.strct.IsValid() {
		m.slice.Set(m.slice.Slice(0, 0))
	}

	m.strct = m.nextElem()
	m.structTableModel.NewModel()
	return m
}

func (m *sliceTableModel) AfterQuery(db DB) error {
	if !m.table.Has(AfterQueryHookFlag) {
		return nil
	}
	return callAfterQueryHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) AfterSelect(db DB) error {
	if !m.table.Has(AfterSelectHookFlag) {
		return nil
	}
	return callAfterSelectHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) BeforeCreate(db DB) error {
	if !m.table.Has(BeforeCreateHookFlag) {
		return nil
	}
	return callBeforeCreateHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) AfterCreate(db DB) error {
	if !m.table.Has(AfterCreateHookFlag) {
		return nil
	}
	return callAfterCreateHookSlice(m.slice, m.sliceOfPtr, db)
}

func (m *sliceTableModel) nextElem() reflect.Value {
	if m.slice.Len() < m.slice.Cap() {
		m.slice.Set(m.slice.Slice(0, m.slice.Len()+1))
		return m.slice.Index(m.slice.Len() - 1)
	}

	if m.sliceOfPtr {
		elem := reflect.New(m.table.Type)
		m.slice.Set(reflect.Append(m.slice, elem))
		return elem.Elem()
	}

	m.slice.Set(reflect.Append(m.slice, m.zeroElem))
	return m.slice.Index(m.slice.Len() - 1)
}
