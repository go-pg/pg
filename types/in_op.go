package types

import (
	"reflect"
)

type inOp struct {
	slice reflect.Value
}

var _ ValueAppender = (*inOp)(nil)

func InMulti(values ...interface{}) ValueAppender {
	return &inOp{
		slice: reflect.ValueOf(values),
	}
}

func In(slice interface{}) ValueAppender {
	return &inOp{
		slice: reflect.ValueOf(slice),
	}
}

func (in *inOp) AppendValue(b []byte, flags int) ([]byte, error) {
	return appendIn(b, in.slice, flags), nil
}

func appendIn(b []byte, slice reflect.Value, flags int) []byte {
	for i := 0; i < slice.Len(); i++ {
		if i > 0 {
			b = append(b, ',')
		}

		elem := slice.Index(i)
		if elem.Kind() == reflect.Interface {
			elem = elem.Elem()
		}

		if elem.Kind() == reflect.Slice {
			b = append(b, '(')
			b = appendIn(b, elem, flags)
			b = append(b, ')')
		} else {
			b = appendValue(b, elem, flags)
		}
	}
	return b
}
