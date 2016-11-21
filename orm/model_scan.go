package orm

import (
	"fmt"

	"gopkg.in/pg.v5/types"
)

type valuesModel struct {
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

func (valuesModel) AfterQuery(_ DB) error {
	return nil
}

func (valuesModel) AfterSelect(_ DB) error {
	return nil
}

func (valuesModel) BeforeInsert(_ DB) error {
	return nil
}

func (valuesModel) AfterInsert(_ DB) error {
	return nil
}

func (valuesModel) BeforeUpdate(_ DB) error {
	return nil
}

func (valuesModel) AfterUpdate(_ DB) error {
	return nil
}

func (valuesModel) BeforeDelete(_ DB) error {
	return nil
}

func (valuesModel) AfterDelete(_ DB) error {
	return nil
}

func (m valuesModel) ScanColumn(colIdx int, colName string, b []byte) error {
	if colIdx >= len(m.values) {
		return fmt.Errorf("pg: no Scan value for column index=%d name=%s", colIdx, colName)
	}
	return types.Scan(m.values[colIdx], b)
}
