package orm

// Collection is a set of models mapped to database rows.
type Collection interface {
	// NewModel returns ColumnScanner or struct that are used to scan
	// columns from the current row.
	NextModel() interface{}
}

// ColumnScanner is an interface used to scan column.
type ColumnScanner interface {
	// Scan assigns a column value from a row.
	//
	// An error should be returned if the value can not be stored
	// without loss of information.
	ScanColumn(colIdx int, colName string, b []byte) error
}

type QueryAppender interface {
	AppendQuery([]byte, ...interface{}) ([]byte, error)
}
