package orm

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/types"
)

type scanValuesModel struct {
	Discard
	values []interface{}
}

var _ Model = scanValuesModel{}

func Scan(values ...interface{}) scanValuesModel {
	return scanValuesModel{
		values: values,
	}
}

func (scanValuesModel) useQueryOne() bool {
	return true
}

func (m scanValuesModel) NewModel() ColumnScanner {
	return m
}

func (m scanValuesModel) ScanColumn(colIdx int, colName string, b []byte) error {
	if colIdx >= len(m.values) {
		return fmt.Errorf("pg: no Scan var for column index=%d name=%q",
			colIdx, colName)
	}
	return types.Scan(m.values[colIdx], b)
}

//------------------------------------------------------------------------------

type scanReflectValuesModel struct {
	Discard
	values []reflect.Value
}

var _ Model = scanReflectValuesModel{}

func scanReflectValues(values []reflect.Value) scanReflectValuesModel {
	return scanReflectValuesModel{
		values: values,
	}
}

func (scanReflectValuesModel) useQueryOne() bool {
	return true
}

func (m scanReflectValuesModel) NewModel() ColumnScanner {
	return m
}

func (m scanReflectValuesModel) ScanColumn(colIdx int, colName string, b []byte) error {
	if colIdx >= len(m.values) {
		return fmt.Errorf("pg: no Scan var for column index=%d name=%q",
			colIdx, colName)
	}
	return types.ScanValue(m.values[colIdx], b)
}
