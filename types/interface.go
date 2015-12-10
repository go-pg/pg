package types

type ValueAppender interface {
	// TODO(vmihailenco): add ability to return error
	AppendValue([]byte, bool) []byte
}
