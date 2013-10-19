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

type Appender interface {
	Append([]byte) []byte
}

// Raw query.
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
