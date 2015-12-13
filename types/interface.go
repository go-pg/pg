package types

type ValueAppender interface {
	// TODO(vmihailenco): add ability to return error
	AppendValue([]byte, bool) []byte
}

//------------------------------------------------------------------------------

// Q is a ValueAppender that represents safe SQL query.
type Q string

var _ ValueAppender = Q("")

func (q Q) AppendValue(dst []byte, quote bool) []byte {
	return append(dst, string(q)...)
}

//------------------------------------------------------------------------------

// F is a ValueAppender that represents SQL field, e.g. table or column name.
type F string

var _ ValueAppender = F("")

func (f F) AppendValue(dst []byte, quote bool) []byte {
	return AppendField(dst, string(f))
}
