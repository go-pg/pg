package pg // import "gopkg.in/pg.v3"

// Collection is a set of records mapped to database rows.
type Collection interface {
	// NewRecord returns ColumnLoader or struct that are used to scan
	// columns from the current row.
	NewRecord() interface{}
}

// ColumnLoader is an interface used by LoadColumn.
//
// TODO(vmihailenco): rename to ColumnScanner
type ColumnLoader interface {
	// Scan assigns a column value from a row.
	//
	// An error should be returned if the value can not be stored
	// without loss of information.
	//
	// TODO(vmihailenco): rename to ScanColumn
	LoadColumn(colIdx int, colName string, b []byte) error
}

type QueryAppender interface {
	// TODO(vmihailenco): add ability to return error
	AppendQuery([]byte) []byte
}

type RawQueryAppender interface {
	// TODO(vmihailenco): add ability to return error
	AppendRawQuery([]byte) []byte
}

// Q is a QueryAppender that represents safe SQL query.
type Q string

var _ QueryAppender = Q("")
var _ RawQueryAppender = Q("")

func (q Q) AppendQuery(dst []byte) []byte {
	return append(dst, string(q)...)
}

func (q Q) AppendRawQuery(dst []byte) []byte {
	return q.AppendQuery(dst)
}

// F is a QueryAppender that represents SQL field, e.g. table or column name.
type F string

var _ QueryAppender = F("")

func (f F) AppendQuery(dst []byte) []byte {
	return appendField(dst, string(f))
}
