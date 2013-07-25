package pg

import (
	"log"
	"os"
)

var Logger = log.New(os.Stdout, "pg: ", log.Ldate|log.Ltime)

type Fabric interface {
	New() interface{}
}

type fabricWrapper struct {
	model interface{}
}

func (f *fabricWrapper) New() interface{} {
	return f.model
}

type Loader interface {
	Load(i int, b []byte) error
}

type Appender interface {
	Append([]byte) []byte
}

// Raw query.
type Q struct {
	b   []byte
	err error
}

func NewQ(q string, args ...interface{}) *Q {
	b, err := FormatQuery(nil, []byte(q), args...)
	return &Q{
		b:   b,
		err: err,
	}
}

func (q *Q) Append(dst []byte) []byte {
	if q == nil {
		return dst
	}
	if q.err != nil {
		panic(q.err)
	}
	dst = append(dst, q.b...)
	return dst
}

// SQL field.
type F []byte

func (f F) Append(dst []byte) []byte {
	return appendPgField(dst, f)
}
