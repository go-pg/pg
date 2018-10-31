package types

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/internal"
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
	return func(v reflect.Value, rd Reader, n int) error {
		if !v.CanSet() {
			return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
		}

		if n == -1 {
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

		p := newArrayParser(rd)
		nextValue := internal.MakeSliceNextElemFunc(v)
		var elemRd *internal.BytesReader

		for p.Valid() {
			elem, err := p.NextElem()
			if err != nil {
				if err == endOfArray {
					break
				}
				return err
			}

			if elemRd == nil {
				elemRd = internal.NewBytesReader(elem)
			} else {
				elemRd.Reset(elem)
			}

			var elemN int
			if elem == nil {
				elemN = -1
			} else {
				elemN = len(elem)
			}

			elemValue := nextValue()
			err = scanElem(elemValue, elemRd, elemN)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func scanSliceStringValue(v reflect.Value, rd Reader, n int) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}

	strings, err := decodeSliceString(rd, n)
	if err != nil {
		return err
	}

	v.Set(reflect.ValueOf(strings))
	return nil
}

func decodeSliceString(rd Reader, n int) ([]string, error) {
	if n == -1 {
		return nil, nil
	}

	p := newArrayParser(rd)
	slice := make([]string, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			if err == endOfArray {
				break
			}
			return nil, err
		}

		slice = append(slice, string(elem))
	}

	return slice, nil
}

func scanSliceIntValue(v reflect.Value, rd Reader, n int) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}

	slice, err := decodeSliceInt(rd, n)
	if err != nil {
		return err
	}

	v.Set(reflect.ValueOf(slice))
	return nil
}

func decodeSliceInt(rd Reader, n int) ([]int, error) {
	if n == -1 {
		return nil, nil
	}

	p := newArrayParser(rd)
	slice := make([]int, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			if err == endOfArray {
				break
			}
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

func scanSliceInt64Value(v reflect.Value, rd Reader, n int) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}

	slice, err := decodeSliceInt64(rd, n)
	if err != nil {
		return err
	}

	v.Set(reflect.ValueOf(slice))
	return nil
}

func decodeSliceInt64(rd Reader, n int) ([]int64, error) {
	if n == -1 {
		return nil, nil
	}

	p := newArrayParser(rd)
	slice := make([]int64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			if err == endOfArray {
				break
			}
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

func scanSliceFloat64Value(v reflect.Value, rd Reader, n int) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}

	slice, err := decodeSliceFloat64(rd, n)
	if err != nil {
		return err
	}

	v.Set(reflect.ValueOf(slice))
	return nil
}

func decodeSliceFloat64(rd Reader, n int) ([]float64, error) {
	if n == -1 {
		return nil, nil
	}

	p := newArrayParser(rd)
	slice := make([]float64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			if err == endOfArray {
				break
			}
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
