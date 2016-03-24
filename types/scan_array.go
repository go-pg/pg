package types

import (
	"fmt"
	"reflect"
	"strconv"
)

var sliceScanner = []valueScanner{
	reflect.Bool:          nil,
	reflect.Int:           scanIntSliceValue,
	reflect.Int8:          nil,
	reflect.Int16:         nil,
	reflect.Int32:         nil,
	reflect.Int64:         scanInt64SliceValue,
	reflect.Uint:          nil,
	reflect.Uint8:         nil,
	reflect.Uint16:        nil,
	reflect.Uint32:        nil,
	reflect.Uint64:        nil,
	reflect.Uintptr:       nil,
	reflect.Float32:       nil,
	reflect.Float64:       scanFloat64SliceValue,
	reflect.Complex64:     nil,
	reflect.Complex128:    nil,
	reflect.Array:         nil,
	reflect.Chan:          nil,
	reflect.Func:          nil,
	reflect.Interface:     nil,
	reflect.Map:           nil,
	reflect.Ptr:           nil,
	reflect.Slice:         nil,
	reflect.String:        scanStringSliceValue,
	reflect.Struct:        nil,
	reflect.UnsafePointer: nil,
}

func arrayScanner(v reflect.Value) valueScanner {
	elemType := v.Type().Elem()

	if scanner := sliceScanner[elemType.Kind()]; scanner != nil {
		return scanner
	}

	scanElem := Scanner(elemType)
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

func scanStringSliceValue(v reflect.Value, b []byte) error {
	strings, err := scanStringSlice(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(strings))
	return nil
}

func scanStringSlice(b []byte) ([]string, error) {
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

func scanIntSliceValue(v reflect.Value, b []byte) error {
	ints, err := scanIntSlice(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(ints))
	return nil
}

func scanIntSlice(b []byte) ([]int, error) {
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

func scanInt64SliceValue(v reflect.Value, b []byte) error {
	ints, err := scanInt64Slice(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(ints))
	return nil
}

func scanInt64Slice(b []byte) ([]int64, error) {
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

func scanFloat64SliceValue(v reflect.Value, b []byte) error {
	floats, err := scanFloat64Slice(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(floats))
	return nil
}

func scanFloat64Slice(b []byte) ([]float64, error) {
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
