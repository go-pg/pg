package types

import (
	"database/sql"
	"fmt"
	"reflect"
)

type Hstore struct {
	v reflect.Value

	append AppenderFunc
	scan   ScannerFunc
}

var _ ValueAppender = (*Hstore)(nil)
var _ sql.Scanner = (*Hstore)(nil)

func NewHstore(vi interface{}) *Hstore {
	v := reflect.ValueOf(vi)
	if !v.IsValid() {
		panic(fmt.Errorf("pg.Hstore(nil)"))
	}
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Map {
		panic(fmt.Errorf("pg.Hstore(unsupported %s)", v.Type()))
	}
	return &Hstore{
		v: v,

		append: HstoreAppender(v.Type()),
		scan:   HstoreScanner(v.Type()),
	}
}

func (h *Hstore) Value() interface{} {
	if h.v.IsValid() {
		return h.v.Interface()
	}
	return nil
}

func (h *Hstore) AppendValue(b []byte, quote int) []byte {
	return h.append(b, h.v, quote)
}

func (h *Hstore) Scan(b interface{}) error {
	if b == nil {
		return h.scan(h.v, nil)
	}
	return h.scan(h.v, b.([]byte))
}
