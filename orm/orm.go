package orm

// Collection is a set of models mapped to database rows.
type Collection interface {
	// NewModel returns ColumnScanner or struct that are used to scan
	// columns from the current row.
	NextModel() interface{}
}

type QueryAppender interface {
	AppendQuery([]byte, ...interface{}) ([]byte, error)
}
