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
	m.slice = bind.Field(m.path[len(m.path)-1])
}

func (m *sliceTableModel) Value() reflect.Value {
	return m.slice
}

func (m *sliceTableModel) NewModel() ColumnScanner {
	m.strct = internal.SliceNextElem(m.slice)
	m.structTableModel.NewModel()
	return m
}
