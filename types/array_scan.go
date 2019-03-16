package types

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/internal"
)

var arrayValueScannerType = reflect.TypeOf((*ArrayValueScanner)(nil)).Elem()

type ArrayValueScanner interface {
	BeforeScanArrayValue(rd Reader, n int) error
	ScanArrayValue(rd Reader, n int) error
	AfterScanArrayValue() error
}

func ArrayScanner(typ reflect.Type) ScannerFunc {
	if typ.Implements(arrayValueScannerType) {
		return scanArrayValueScannerValue
	}

	kind := typ.Kind()
	if kind == reflect.Ptr {
		typ = typ.Elem()
		kind = typ.Kind()
	}

	switch kind {
	case reflect.Slice, reflect.Array:
		// ok:
	default:
		return nil
	}

	elemType := typ.Elem()

	if kind == reflect.Slice {
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
	}

	scanElem := scanner(elemType, true)
	return func(v reflect.Value, rd Reader, n int) error {
		v = reflect.Indirect(v)
		if !v.CanSet() {
			return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
		}

		kind := v.Kind()

		if n == -1 {
			if kind != reflect.Slice || !v.IsNil() {
				v.Set(reflect.Zero(v.Type()))
			}
			return nil
		}

		if kind == reflect.Slice {
			if v.IsNil() {
				v.Set(reflect.MakeSlice(v.Type(), 0, 0))
			} else if v.Len() > 0 {
				v.Set(v.Slice(0, 0))
			}
		}

		p := newArrayParser(rd)
		nextValue := internal.MakeSliceNextElemFunc(v)
		var elemRd *BytesReader

		for {
			elem, err := p.NextElem()
			if err != nil {
				if err == endOfArray {
					break
				}
				return err
			}

			if elemRd == nil {
				elemRd = NewBytesReader(elem)
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
	v = reflect.Indirect(v)
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
	for {
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
	v = reflect.Indirect(v)
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
	for {
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
	v = reflect.Indirect(v)
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
	for {
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
	v = reflect.Indirect(v)
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
	for {
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

func scanArrayValueScannerValue(v reflect.Value, rd Reader, n int) error {
	if n == -1 {
		if !v.IsNil() {
			if !v.CanSet() {
				return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
			}
			v.Set(reflect.Zero(v.Type()))
		}
		return nil
	}

	if v.IsNil() {
		if !v.CanSet() {
			return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
		}
		v.Set(reflect.New(v.Type().Elem()))
	}

	scanner := v.Interface().(ArrayValueScanner)
	err := scanner.BeforeScanArrayValue(rd, n)
	if err != nil {
		return err
	}

	p := newArrayParser(rd)
	var elemRd *BytesReader
	for {
		elem, err := p.NextElem()
		if err != nil {
			if err == endOfArray {
				break
			}
			return err
		}

		if elemRd == nil {
			elemRd = NewBytesReader(elem)
		} else {
			elemRd.Reset(elem)
		}

		var elemN int
		if elem == nil {
			elemN = -1
		} else {
			elemN = len(elem)
		}

		err = scanner.ScanArrayValue(elemRd, elemN)
		if err != nil {
			return err
		}
	}

	return scanner.AfterScanArrayValue()
}
