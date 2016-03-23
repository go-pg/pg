package types

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

var (
	scannerType      = reflect.TypeOf(new(sql.Scanner)).Elem()
	driverValuerType = reflect.TypeOf(new(driver.Valuer)).Elem()
)

var (
	timePtrType = reflect.TypeOf((*time.Time)(nil))
	timeType    = timePtrType.Elem()
)

type valueScanner func(reflect.Value, []byte) error

var valueScanners []valueScanner

func init() {
	valueScanners = []valueScanner{
		reflect.Bool:          scanBoolValue,
		reflect.Int:           scanIntValue,
		reflect.Int8:          scanIntValue,
		reflect.Int16:         scanIntValue,
		reflect.Int32:         scanIntValue,
		reflect.Int64:         scanIntValue,
		reflect.Uint:          scanUintValue,
		reflect.Uint8:         scanUintValue,
		reflect.Uint16:        scanUintValue,
		reflect.Uint32:        scanUintValue,
		reflect.Uint64:        scanUintValue,
		reflect.Uintptr:       nil,
		reflect.Float32:       scanFloatValue,
		reflect.Float64:       scanFloatValue,
		reflect.Complex64:     nil,
		reflect.Complex128:    nil,
		reflect.Array:         nil,
		reflect.Chan:          nil,
		reflect.Func:          nil,
		reflect.Interface:     scanInterfaceValue,
		reflect.Map:           scanJSONValue,
		reflect.Ptr:           scanPtrValue,
		reflect.Slice:         scanJSONValue,
		reflect.String:        scanStringValue,
		reflect.Struct:        scanJSONValue,
		reflect.UnsafePointer: nil,
	}
}

func Scanner(typ reflect.Type) valueScanner {
	if typ == timeType {
		return scanTimeValue
	}

	if reflect.PtrTo(typ).Implements(scannerType) {
		return scanSQLScannerAddrValue
	}

	if typ.Implements(scannerType) {
		return scanSQLScannerValue
	}

	kind := typ.Kind()

	if kind == reflect.Slice && typ.Elem().Kind() == reflect.Uint8 {
		return scanBytesValue
	}

	return valueScanners[kind]
}

func ScanValue(v reflect.Value, b []byte) error {
	if !v.IsValid() {
		return fmt.Errorf("pg: Scan(nil)")
	}

	scanner := Scanner(v.Type())
	if scanner != nil {
		return scanner(v, b)
	}

	if v.Kind() == reflect.Interface {
		return fmt.Errorf("pg: Scan(nil)")
	}
	return fmt.Errorf("pg: Scan(unsupported %s)", v.Type())
}

func scanBoolValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	if b == nil {
		v.SetBool(false)
		return nil
	}
	v.SetBool(len(b) == 1 && b[0] == 't')
	return nil
}

func scanIntValue(v reflect.Value, b []byte) error {
	if b == nil {
		v.SetInt(0)
		return nil
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func scanUintValue(v reflect.Value, b []byte) error {
	if b == nil {
		v.SetUint(0)
		return nil
	}
	n, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return err
	}
	v.SetUint(n)
	return nil
}

func scanFloatValue(v reflect.Value, b []byte) error {
	if b == nil {
		v.SetFloat(0)
		return nil
	}
	n, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return err
	}
	v.SetFloat(n)
	return nil
}

func scanStringValue(v reflect.Value, b []byte) error {
	v.SetString(string(b))
	return nil
}

func scanJSONValue(v reflect.Value, b []byte) error {
	if b == nil {
		v.Set(reflect.New(v.Type()).Elem())
		return nil
	}
	return json.Unmarshal(b, v.Addr().Interface())
}

func scanTimeValue(v reflect.Value, b []byte) error {
	if b == nil {
		// TODO: cache?
		v.Set(reflect.ValueOf(time.Time{}))
		return nil
	}
	tm, err := ParseTime(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(tm))
	return nil
}

func scanPtrValue(v reflect.Value, b []byte) error {
	if v.IsNil() {
		if !v.CanSet() {
			return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
		}
		if b == nil {
			return nil
		}
		vv := reflect.New(v.Type().Elem())
		v.Set(vv)
	}
	return ScanValue(v.Elem(), b)
}

func scanBytesValue(v reflect.Value, b []byte) error {
	if b == nil {
		v.SetBytes(nil)
		return nil
	}
	bs, err := scanBytes(b)
	if err != nil {
		return err
	}
	v.SetBytes(bs)
	return nil
}

func scanInterfaceValue(v reflect.Value, b []byte) error {
	if v.IsNil() {
		return fmt.Errorf("pg: Scan(nil)")
	}
	return ScanValue(v.Elem(), b)
}

func scanMapValue(v reflect.Value, b []byte) error {
	typ := v.Type()
	if typ.Key().Kind() == reflect.String && typ.Elem().Kind() == reflect.String {
		m, err := scanStringStringMap(b)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(m))
		return nil
	}
	return fmt.Errorf("pg: Scan(unsupported %s)", v.Type())
}

func scanSQLScannerValue(v reflect.Value, b []byte) error {
	if b == nil {
		if v.IsNil() {
			return nil
		}
		return scanSQLScanner(v.Interface().(sql.Scanner), nil)
	}
	if v.IsNil() {
		v.Set(reflect.New(v.Type().Elem()))
	}
	return scanSQLScanner(v.Interface().(sql.Scanner), b)
}

func scanSQLScannerAddrValue(v reflect.Value, b []byte) error {
	return scanSQLScanner(v.Addr().Interface().(sql.Scanner), b)
}
