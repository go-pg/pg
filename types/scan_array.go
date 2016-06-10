package types

import (
	"reflect"
	"strconv"

	"gopkg.in/pg.v4/internal"
	"gopkg.in/pg.v4/internal/parser"
)

var sliceScanner = []ScannerFunc{
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

func ArrayScanner(typ reflect.Type) ScannerFunc {
	elemType := typ.Elem()

	if scanner := sliceScanner[elemType.Kind()]; scanner != nil {
		return scanner
	}

	scanElem := scanner(elemType, true)
	return func(v reflect.Value, b []byte) error {
		if !v.CanSet() {
			return internal.Errorf("pg: Scan(non-pointer %s)", v.Type())
		}
		if b == nil {
			if !v.IsNil() {
				v.Set(reflect.New(v.Type()))
			}
			return nil
		}
		if v.IsNil() {
			v.Set(reflect.MakeSlice(v.Type(), 0, 0))
		}
		p := parser.NewArrayParser(b)
		for p.Valid() {
			elem, err := p.NextElem()
			if err != nil {
				return err
			}
			elemValue := internal.SliceNextElem(v)
			if err := scanElem(elemValue, elem); err != nil {
				return err
			}
		}
		return nil
	}
}

func scanStringSliceValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return internal.Errorf("pg: Scan(non-pointer %s)", v.Type())
	}
	strings, err := decodeStringSlice(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(strings))
	return nil
}

func decodeStringSlice(b []byte) ([]string, error) {
	if b == nil {
		return nil, nil
	}
	p := parser.NewArrayParser(b)
	s := make([]string, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		s = append(s, string(elem))
	}
	return s, nil
}

func scanIntSliceValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return internal.Errorf("pg: Scan(non-pointer %s)", v.Type())
	}
	ints, err := decodeIntSlice(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(ints))
	return nil
}

func decodeIntSlice(b []byte) ([]int, error) {
	if b == nil {
		return nil, nil
	}
	p := parser.NewArrayParser(b)
	slice := make([]int, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			slice = append(slice, 0)
			continue
		}
		n, err := strconv.Atoi(string(elem))
		if err != nil {
			return nil, err
		}
		slice = append(slice, n)
	}
	return slice, nil
}

func scanInt64SliceValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return internal.Errorf("pg: Scan(non-pointer %s)", v.Type())
	}
	ints, err := decodeInt64Slice(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(ints))
	return nil
}

func decodeInt64Slice(b []byte) ([]int64, error) {
	if b == nil {
		return nil, nil
	}
	p := parser.NewArrayParser(b)
	slice := make([]int64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			slice = append(slice, 0)
			continue
		}
		n, err := strconv.ParseInt(string(elem), 10, 64)
		if err != nil {
			return nil, err
		}
		slice = append(slice, n)
	}
	return slice, nil
}

func scanFloat64SliceValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return internal.Errorf("pg: Scan(non-pointer %s)", v.Type())
	}
	floats, err := decodeFloat64Slice(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(floats))
	return nil
}

func decodeFloat64Slice(b []byte) ([]float64, error) {
	if b == nil {
		return nil, nil
	}
	p := parser.NewArrayParser(b)
	slice := make([]float64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			slice = append(slice, 0)
			continue
		}
		n, err := strconv.ParseFloat(string(elem), 64)
		if err != nil {
			return nil, err
		}
		slice = append(slice, n)
	}
	return slice, nil
}
