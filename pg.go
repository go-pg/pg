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

// Raw SQL query.
type Q string

func (q Q) Append(dst []byte) []byte {
	dst = append(dst, string(q)...)
	return dst
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
