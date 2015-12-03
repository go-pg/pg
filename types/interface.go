package types

type QueryAppender interface {
	// TODO(vmihailenco): add ability to return error
	AppendQuery([]byte) []byte
}
