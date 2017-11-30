package types

import (
	"fmt"
	"reflect"
)

type InOp struct {
	slice reflect.Value
}

var _ ValueAppender = (*InOp)(nil)

func In(values ...interface{}) *InOp {
	var v reflect.Value
	if len(values) == 1 {
		v = reflect.ValueOf(values[0])
	} else {
		v = reflect.ValueOf(values)
	}

	if !v.IsValid() {
		panic(fmt.Errorf("pg.In(nil)"))
	}
	if v.Kind() != reflect.Slice {
		panic(fmt.Errorf("pg.In(unsupported %s)", v.Type()))
	}

	return &InOp{
		slice: v,
	}
}

func (in *InOp) AppendValue(b []byte, quote int) ([]byte, error) {
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
