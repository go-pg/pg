package internal

import (
	"io"
)

var Json JsonProvider

type JsonProvider interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
	NewEncoder(writer io.Writer) JsonEncoder
	NewDecoder(reader io.Reader) JsonDecoder
}

type JsonDecoder interface {
	Decode(v interface{}) error
	UseNumber()
}

type JsonEncoder interface {
	Encode(v interface{}) error
}
