package orm

import (
	"reflect"

	"github.com/go-pg/pg/internal"
)

type sliceModel struct {
	Discard
	slice    reflect.Value
	nextElem func() reflect.Value
	scan     func(reflect.Value, []byte) error
}

var _ Model = (*sliceModel)(nil)

func (m *sliceModel) Init() error {
	if m.slice.IsValid() && m.slice.Len() > 0 {
		m.slice.Set(m.slice.Slice(0, 0))
	}
	return nil
}

func (m *sliceModel) NewModel() ColumnScanner {
	return m
}

func (m *sliceModel) ScanColumn(colIdx int, _ string, b []byte) error {
	if m.nextElem == nil {
		m.nextElem = internal.MakeSliceNextElemFunc(m.slice)
	}
	v := m.nextElem()
	return m.scan(v, b)
}
