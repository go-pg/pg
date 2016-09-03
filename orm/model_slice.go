package orm

import (
	"reflect"

	"gopkg.in/pg.v4/internal"
)

type sliceModel struct {
	slice reflect.Value
	scan  func(reflect.Value, []byte) error
}

var _ Model = (*sliceModel)(nil)

func (m *sliceModel) NewModel(_ DB) ColumnScanner {
	return m
}

func (sliceModel) AddModel(_ DB, _ ColumnScanner) error {
	return nil
}

func (m *sliceModel) ScanColumn(colIdx int, _ string, b []byte) error {
	v := internal.SliceNextElem(m.slice)
	return m.scan(v, b)
}
