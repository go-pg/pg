package orm

import (
	"fmt"

	"github.com/go-pg/pg/types"
)

type valuesModel struct {
	hookStubs
	values []interface{}
}

var _ Model = valuesModel{}

func Scan(values ...interface{}) valuesModel {
	return valuesModel{
		values: values,
	}
}

func (valuesModel) useQueryOne() bool {
	return true
}

func (valuesModel) Reset() error {
	return nil
}

func (m valuesModel) NewModel() ColumnScanner {
	return m
}

func (valuesModel) AddModel(_ ColumnScanner) error {
	return nil
}

func (m valuesModel) ScanColumn(colIdx int, colName string, b []byte) error {
	if colIdx >= len(m.values) {
		return fmt.Errorf(
			"pg: no Scan var for column index=%d name=%q",
			colIdx, colName,
		)
	}
	return types.Scan(m.values[colIdx], b)
}
