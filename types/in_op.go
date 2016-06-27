package types

import (
	"fmt"
	"reflect"
)

type InOp struct {
	slice  reflect.Value
	append AppenderFunc
}

var _ ValueAppender = (*InOp)(nil)

func In(slice interface{}) *InOp {
	v := reflect.ValueOf(slice)
	if !v.IsValid() {
		panic(fmt.Errorf("pg.In(nil)"))
	}
	if v.Kind() != reflect.Slice {
		panic(fmt.Errorf("pg.In(unsupported %s)", v.Type()))
	}
	return &InOp{
		slice:  v,
		append: Appender(v.Type().Elem()),
	}
}

func (in *InOp) AppendValue(b []byte, quote int) ([]byte, error) {
	for i := 0; i < in.slice.Len(); i++ {
		b = in.append(b, in.slice.Index(i), quote)
		b = append(b, ',')
	}
	if in.slice.Len() > 0 {
		b = b[:len(b)-1]
	}
	return b, nil
}
