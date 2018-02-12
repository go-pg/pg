package types

import (
	"database/sql"
	"fmt"
	"reflect"
)

type Array struct {
	v reflect.Value

	append AppenderFunc
	scan   ScannerFunc
}

var _ ValueAppender = (*Array)(nil)
var _ sql.Scanner = (*Array)(nil)

func NewArray(vi interface{}) *Array {
	v := reflect.ValueOf(vi)
	if !v.IsValid() {
		panic(fmt.Errorf("pg.Array(nil)"))
	}
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Slice {
		panic(fmt.Errorf("pg.Array(unsupported %s)", v.Type()))
	}
	return &Array{
		v: v,

		append: ArrayAppender(v.Type()),
		scan:   ArrayScanner(v.Type()),
	}
}

func (a *Array) Value() interface{} {
	if a.v.IsValid() {
		return a.v.Interface()
	}
	return nil
}

func (a *Array) AppendValue(b []byte, quote int) []byte {
	return a.append(b, a.v, quote)
}

func (a *Array) Scan(b interface{}) error {
	if b == nil {
		return a.scan(a.v, nil)
	}
	return a.scan(a.v, b.([]byte))
}
