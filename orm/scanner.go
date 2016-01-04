package orm

import "gopkg.in/pg.v3/types"

// ColumnScanner is an interface used to scan column.
type ColumnScanner interface {
	// Scan assigns a column value from a row.
	//
	// An error should be returned if the value can not be stored
	// without loss of information.
	ScanColumn(colIdx int, colName string, b []byte) error
}

type valuesScanner struct {
	values []interface{}
}

var _ ColumnScanner = (*valuesScanner)(nil)
var _ Collection = (*valuesScanner)(nil)

// Scan returns ColumnScanner that copies the columns in the
// row into the values.
func Scan(values ...interface{}) ColumnScanner {
	return &valuesScanner{values}
}

func (s *valuesScanner) NextModel() interface{} {
	return s
}

func (s *valuesScanner) ScanColumn(colIdx int, _ string, b []byte) error {
	return types.Decode(s.values[colIdx], b)
}
