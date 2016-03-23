package types

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
)

func arrayAppender(v reflect.Value) valueAppender {
	appendElem := Appender(v.Type().Elem())
	return func(b []byte, v reflect.Value, quote int) []byte {
		if v.IsNil() {
			return AppendNull(b, quote)
		}

		if quote == 1 {
			b = append(b, '\'')
		}

		b = append(b, '{')
		for i := 0; i < v.Len(); i++ {
			elem := v.Index(i)
			b = appendElem(b, elem, 2)
			b = append(b, ',')
		}
		if v.Len() > 0 {
			b[len(b)-1] = '}' // Replace trailing comma.
		} else {
			b = append(b, '}')
		}

		if quote == 1 {
			b = append(b, '\'')
		}

		return b
	}
}

func arrayScanner(v reflect.Value) valueDecoder {
	scanElem := Decoder(v.Type().Elem())
	return func(v reflect.Value, b []byte) error {
		p := newArrayParser(b)
		if v.IsNil() {
			v.Set(reflect.MakeSlice(v.Type(), 0, 0))
		}
		for p.Valid() {
			elem, err := p.NextElem()
			if err != nil {
				return err
			}
			if elem == nil {
				return fmt.Errorf("pg: unexpected NULL: %q", b)
			}
			elemValue := sliceNextElem(v)
			if err := scanElem(elemValue, elem); err != nil {
				return err
			}
		}
		return nil
	}
}

func sliceNextElem(v reflect.Value) reflect.Value {
	if v.Type().Elem().Kind() == reflect.Ptr {
		elem := reflect.New(v.Type().Elem().Elem())
		v.Set(reflect.Append(v, elem))
		return elem.Elem()
	}

	elem := reflect.New(v.Type().Elem()).Elem()
	v.Set(reflect.Append(v, elem))
	elem = v.Index(v.Len() - 1)
	return elem
}

type Array struct {
	v reflect.Value

	append valueAppender
	scan   valueDecoder
}

var _ ValueAppender = (*Array)(nil)
var _ sql.Scanner = (*Array)(nil)

func NewArray(vi interface{}) *Array {
	v := reflect.ValueOf(vi)
	if !v.IsValid() {
		panic(fmt.Errorf("pg.Array(nil)", v.Type()))
	}
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Slice {
		panic(fmt.Errorf("pg.Array(unsupported %s)", v.Type()))
	}
	return &Array{
		v: v,

		append: arrayAppender(v),
		scan:   arrayScanner(v),
	}
}

func (a *Array) Value() interface{} {
	if a.v.IsValid() {
		return a.v.Interface()
	}
	return nil
}

func (a *Array) AppendValue(b []byte, quote int) ([]byte, error) {
	b = a.append(b, a.v, quote)
	return b, nil
}

func (a *Array) Scan(b interface{}) error {
	if b == nil {
		return nil
	}
	return a.scan(a.v, b.([]byte))
}

func appendStringSlice(b []byte, ss []string, quote int) []byte {
	if ss == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, s := range ss {
		b = AppendString(b, s, 2)
		b = append(b, ',')
	}
	if len(ss) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func appendIntSlice(b []byte, ints []int, quote int) []byte {
	if ints == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, n := range ints {
		b = strconv.AppendInt(b, int64(n), 10)
		b = append(b, ',')
	}
	if len(ints) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func appendInt64Slice(b []byte, ints []int64, quote int) []byte {
	if ints == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, "{"...)
	for _, n := range ints {
		b = strconv.AppendInt(b, n, 10)
		b = append(b, ',')
	}
	if len(ints) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func appendFloat64Slice(b []byte, floats []float64, quote int) []byte {
	if floats == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, "{"...)
	for _, n := range floats {
		b = appendFloat(b, n)
		b = append(b, ',')
	}
	if len(floats) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func decodeStringSlice(b []byte) ([]string, error) {
	p := newArrayParser(b)
	s := make([]string, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}
		s = append(s, string(elem))
	}
	return s, nil
}

func decodeIntSlice(b []byte) ([]int, error) {
	p := newArrayParser(b)
	s := make([]int, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}
		n, err := strconv.Atoi(string(elem))
		if err != nil {
			return nil, err
		}
		s = append(s, n)
	}
	return s, nil
}

func decodeInt64Slice(b []byte) ([]int64, error) {
	p := newArrayParser(b)
	s := make([]int64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}
		n, err := strconv.ParseInt(string(elem), 10, 64)
		if err != nil {
			return nil, err
		}
		s = append(s, n)
	}
	return s, nil
}

func decodeFloat64Slice(b []byte) ([]float64, error) {
	p := newArrayParser(b)
	slice := make([]float64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", b)
		}
		n, err := strconv.ParseFloat(string(elem), 64)
		if err != nil {
			return nil, err
		}
		slice = append(slice, n)
	}
	return slice, nil
}
