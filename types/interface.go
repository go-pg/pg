package types

type ValueAppender interface {
	AppendValue(b []byte, quote int) ([]byte, error)
}

//------------------------------------------------------------------------------

// Q represents safe SQL query.
type Q string

var _ ValueAppender = Q("")

func (q Q) AppendValue(b []byte, quote int) ([]byte, error) {
	return append(b, q...), nil
}

//------------------------------------------------------------------------------

// F represents a SQL field, e.g. table or column name.
type F string

var _ ValueAppender = F("")

func (f F) AppendValue(b []byte, quote int) ([]byte, error) {
	return AppendField(b, string(f), quote), nil
}
