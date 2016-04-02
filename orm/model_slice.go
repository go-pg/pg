package orm

import "reflect"

type SliceModel struct {
	StructModel
	slice reflect.Value
}

var _ TableModel = (*SliceModel)(nil)

func (m *SliceModel) Join(name string) *Join {
	return join(&m.StructModel, m.Value(), name)
}

func (m *SliceModel) Kind() reflect.Kind {
	return reflect.Slice
}

func (m *SliceModel) Bind(bind reflect.Value) {
	m.slice = bind.FieldByName(m.path[len(m.path)-1])
}

func (m *SliceModel) Value() reflect.Value {
	return m.slice
}

func (m *SliceModel) NewModel() ColumnScanner {
	m.strct = sliceNextElem(m.slice)
	m.StructModel.NewModel()
	return m
}
