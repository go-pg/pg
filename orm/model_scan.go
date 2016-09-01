package orm

import "gopkg.in/pg.v4/types"

type valuesModel struct {
	values []interface{}
}

var _ ColumnScanner = valuesModel{}
var _ Collection = valuesModel{}

func Scan(values ...interface{}) valuesModel {
	return valuesModel{
		values: values,
	}
}

func (valuesModel) useQueryOne() bool {
	return true
}

func (m valuesModel) NewModel(_ DB) ColumnScanner {
	return m
}

func (valuesModel) AddModel(_ DB, _ ColumnScanner) error {
	return nil
}

func (m valuesModel) ScanColumn(colIdx int, _ string, b []byte) error {
	return types.Scan(m.values[colIdx], b)
}
