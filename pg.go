package pg // import "gopkg.in/pg.v3"

type Collection interface {
	NewRecord() interface{}
}

type ColumnLoader interface {
	LoadColumn(colIdx int, colName string, b []byte) error
}

type QueryAppender interface {
	AppendQuery([]byte) []byte
}

type RawQueryAppender interface {
	AppendRawQuery([]byte) []byte
}

// Raw SQL query.
type Q string

var _ QueryAppender = Q("")
var _ RawQueryAppender = Q("")

func (q Q) AppendQuery(dst []byte) []byte {
	return append(dst, string(q)...)
}

func (q Q) AppendRawQuery(dst []byte) []byte {
	return q.AppendQuery(dst)
}

// SQL field, e.g. table or column name.
type F string

var _ QueryAppender = F("")

func (f F) AppendQuery(dst []byte) []byte {
	dst = append(dst, '"')
	for _, c := range []byte(f) {
		if c == '"' {
			dst = append(dst, '"', '"')
		} else {
			dst = append(dst, c)
		}
	}
	dst = append(dst, '"')
	return dst
}

type RecordReader interface {
	Read() ([]string, error)
}
