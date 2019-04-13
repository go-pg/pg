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

func InSlice(slice interface{}) ValueAppender {
	return &inOp{
		slice: reflect.ValueOf(slice),
	}
}

func (in *inOp) AppendValue(b []byte, quote int) []byte {
	return appendIn(b, in.slice, quote)
}

func appendIn(b []byte, slice reflect.Value, quote int) []byte {
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
			b = appendIn(b, elem, quote)
			b = append(b, ')')
		} else {
			b = appendValue(b, elem, quote)
		}
	}
	return b
}
