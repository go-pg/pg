package orm

import (
	"github.com/go-pg/pg/v10/types"
)

type mapModel struct {
	hookStubs
	ptr *map[string]interface{}
	m   map[string]interface{}
}

var _ Model = (*mapModel)(nil)

func newMapModel(ptr *map[string]interface{}) *mapModel {
	model := &mapModel{
		ptr: ptr,
	}
	if ptr != nil {
		model.m = *ptr
	}
	return model
}

func (mapModel) Init() error {
	return nil
}

func (m *mapModel) NextColumnScanner() ColumnScanner {
	return m
}

func (m mapModel) AddColumnScanner(ColumnScanner) error {
	return nil
}

func (m *mapModel) ScanColumn(col types.ColumnInfo, rd types.Reader, n int) error {
	val, err := types.ReadColumnValue(col, rd, n)
	if err != nil {
		return err
	}

	if m.m == nil {
		m.m = make(map[string]interface{})
		*m.ptr = m.m
	}

	m.m[col.Name] = val
	return nil
}

func (mapModel) useQueryOne() bool {
	return true
}
