package types

import (
	"reflect"
)

type inOp struct {
	slice reflect.Value
}

var _ ValueAppender = (*inOp)(nil)

func In(values ...interface{}) ValueAppender {
	v := reflect.ValueOf(values)
	if v.Len() == 1 {
		vv := v.Index(0).Elem()
		if vv.Kind() == reflect.Slice {
			v = vv
		}
	}
	return &inOp{
		slice: v,
	}
}

func (in *inOp) AppendValue(b []byte, quote int) ([]byte, error) {
	return appendIn(b, in.slice, quote), nil
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
