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

func (m *sliceModel) NewModel() ColumnScanner {
	return m
}

func (sliceModel) AddModel(_ ColumnScanner) error {
	return nil
}

func (sliceModel) AfterQuery(_ DB) error {
	return nil
}

func (sliceModel) AfterSelect(_ DB) error {
	return nil
}

func (sliceModel) BeforeCreate(_ DB) error {
	return nil
}

func (sliceModel) AfterCreate(_ DB) error {
	return nil
}

func (m *sliceModel) ScanColumn(colIdx int, _ string, b []byte) error {
	v := internal.SliceNextElem(m.slice)
	return m.scan(v, b)
}
