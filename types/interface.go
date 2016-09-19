package types

type ValueAppender interface {
	AppendValue(b []byte, quote int) ([]byte, error)
}

//------------------------------------------------------------------------------

// Q represents safe SQL query.
type Q string

var _ ValueAppender = Q("")

func (q Q) AppendValue(dst []byte, quote int) ([]byte, error) {
	return append(dst, q...), nil
}

//------------------------------------------------------------------------------

// F represents a SQL field, e.g. table or column name.
type F string

var _ ValueAppender = F("")

func (f F) AppendValue(dst []byte, quote int) ([]byte, error) {
	return AppendField(dst, string(f), quote), nil
}
