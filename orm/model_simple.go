package orm

import "reflect"

type simpleModel struct {
	slice reflect.Value
	scan  func(reflect.Value, []byte) error
}

func (m *simpleModel) NewModel() ColumnScanner {
	return m
}

func (simpleModel) AddModel(_ ColumnScanner) error {
	return nil
}

func (m *simpleModel) ScanColumn(colIdx int, _ string, b []byte) error {
	v := sliceNextElem(m.slice)
	return m.scan(v, b)
}
