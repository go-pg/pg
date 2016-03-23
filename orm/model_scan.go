package orm

import "gopkg.in/pg.v4/types"

type valuesModel struct {
	values []interface{}
}

var _ ColumnScanner = (*valuesModel)(nil)
var _ Collection = (*valuesModel)(nil)

func Scan(values ...interface{}) ColumnScanner {
	return &valuesModel{values}
}

func (m *valuesModel) NewModel() ColumnScanner {
	return m
}

func (valuesModel) AddModel(_ ColumnScanner) error {
	return nil
}

func (m *valuesModel) ScanColumn(colIdx int, _ string, b []byte) error {
	return types.Scan(m.values[colIdx], b)
}
