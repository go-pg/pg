package pg

type Factory interface {
	New() interface{}
}

type singleFactory struct {
	v interface{}
}

func (f *singleFactory) New() interface{} {
	return f.v
}

type Appender interface {
	Append([]byte) []byte
}

type RawAppender interface {
	AppendRaw([]byte) []byte
}

// Raw SQL query.
type Q string

func (q Q) Append(dst []byte) []byte {
	return append(dst, string(q)...)
}

func (q Q) AppendRaw(dst []byte) []byte {
	return q.Append(dst)
}

// SQL field.
type F string

func (f F) Append(dst []byte) []byte {
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
