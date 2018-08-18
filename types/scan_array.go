package types

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/parser"
)

func ArrayScanner(typ reflect.Type) ScannerFunc {
	elemType := typ.Elem()

	switch elemType {
	case stringType:
		return scanSliceStringValue
	case intType:
		return scanSliceIntValue
	case int64Type:
		return scanSliceInt64Value
	case float64Type:
		return scanSliceFloat64Value
	}

	scanElem := scanner(elemType, true)
	return func(v reflect.Value, b []byte) error {
		if !v.CanSet() {
			return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
		}

		if b == nil {
			if !v.IsNil() {
				v.Set(reflect.Zero(v.Type()))
			}
			return nil
		}

		if v.IsNil() {
			v.Set(reflect.MakeSlice(v.Type(), 0, 0))
		} else if v.Len() > 0 {
			v.Set(v.Slice(0, 0))
		}

		p := parser.NewArrayParser(b)
		nextValue := internal.MakeSliceNextElemFunc(v)
		for p.Valid() {
			elem, err := p.NextElem()
			if err != nil {
				return err
			}

			elemValue := nextValue()
			err = scanElem(elemValue, elem)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func scanSliceStringValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	strings, err := decodeSliceString(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(strings))
	return nil
}

func decodeSliceString(b []byte) ([]string, error) {
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

func scanSliceIntValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	ints, err := decodeSliceInt(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(ints))
	return nil
}

func decodeSliceInt(b []byte) ([]int, error) {
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
		n, err := internal.Atoi(elem)
		if err != nil {
			return nil, err
		}
		slice = append(slice, n)
	}
	return slice, nil
}

func scanSliceInt64Value(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	ints, err := decodeSliceInt64(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(ints))
	return nil
}

func decodeSliceInt64(b []byte) ([]int64, error) {
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
		n, err := internal.ParseInt(elem, 10, 64)
		if err != nil {
			return nil, err
		}
		slice = append(slice, n)
	}
	return slice, nil
}

func scanSliceFloat64Value(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	floats, err := decodeSliceFloat64(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(floats))
	return nil
}

func decodeSliceFloat64(b []byte) ([]float64, error) {
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
		n, err := internal.ParseFloat(elem, 64)
		if err != nil {
			return nil, err
		}
		slice = append(slice, n)
	}
	return slice, nil
}
