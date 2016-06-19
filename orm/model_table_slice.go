package orm

import (
	"reflect"

	"gopkg.in/pg.v4/internal"
)

type sliceTableModel struct {
	structTableModel
	slice reflect.Value
}

var _ tableModel = (*sliceTableModel)(nil)

func (sliceTableModel) useQueryOne() bool {
	return false
}

func (m *sliceTableModel) Join(name string) *join {
	return addJoin(&m.structTableModel, m.Value(), name)
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
	m.strct = internal.SliceNextElem(m.slice)
	m.structTableModel.NewModel()
	return m
}
