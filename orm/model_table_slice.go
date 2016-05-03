package orm

import "reflect"

type sliceTableModel struct {
	structTableModel
	slice reflect.Value
}

var _ tableModel = (*sliceTableModel)(nil)

func (sliceTableModel) useQueryOne() bool {
	return false
}

func (m *sliceTableModel) Join(name string) *Join {
	return join(&m.structTableModel, m.Value(), name)
}

func (m *sliceTableModel) Bind(bind reflect.Value) {
	m.slice = bind.Field(m.path[len(m.path)-1])
}

func (m *sliceTableModel) Value() reflect.Value {
	return m.slice
}

func (m *sliceTableModel) NewModel() ColumnScanner {
	m.strct = sliceNextElem(m.slice)
	m.structTableModel.NewModel()
	return m
}
