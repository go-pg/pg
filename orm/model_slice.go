package orm

import "reflect"

type sliceModel struct {
	slice reflect.Value
	scan  func(reflect.Value, []byte) error
}

var _ Model = (*sliceModel)(nil)

func (m *sliceModel) NewModel() ColumnScanner {
	return m
}

func (sliceModel) AddModel(_ ColumnScanner) error {
	return nil
}

func (m *sliceModel) ScanColumn(colIdx int, _ string, b []byte) error {
	v := sliceNextElem(m.slice)
	return m.scan(v, b)
}
