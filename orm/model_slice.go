package orm

import "reflect"

type sliceModel struct {
	slice   reflect.Value
	decoder func(reflect.Value, []byte) error
}

func (m *sliceModel) NewModel() ColumnScanner {
	return m
}

func (m *sliceModel) ScanColumn(colIdx int, _ string, b []byte) error {
	v := sliceNextElemValue(m.slice)
	return m.decoder(v, b)
}
