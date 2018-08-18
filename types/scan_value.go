package types

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/go-pg/pg/internal"
)

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
var ipType = reflect.TypeOf((*net.IP)(nil)).Elem()
var ipNetType = reflect.TypeOf((*net.IPNet)(nil)).Elem()

type ScannerFunc func(reflect.Value, []byte) error

var valueScanners []ScannerFunc

func init() {
	valueScanners = []ScannerFunc{
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
		reflect.Interface:     scanIfaceValue,
		reflect.Map:           scanJSONValue,
		reflect.Ptr:           nil,
		reflect.Slice:         scanJSONValue,
		reflect.String:        scanStringValue,
		reflect.Struct:        scanJSONValue,
		reflect.UnsafePointer: nil,
	}
}

func Scanner(typ reflect.Type) ScannerFunc {
	return scanner(typ, false)
}

func scanner(typ reflect.Type, pgArray bool) ScannerFunc {
	switch typ {
	case timeType:
		return scanTimeValue
	case ipType:
		return scanIPValue
	case ipNetType:
		return scanIPNetValue
	}

	if typ.Implements(scannerType) {
		return scanSQLScannerValue
	}
	if reflect.PtrTo(typ).Implements(scannerType) {
		return scanSQLScannerAddrValue
	}

	kind := typ.Kind()
	switch kind {
	case reflect.Ptr:
		return ptrScannerFunc(typ)
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			return scanBytesValue
		}
		if pgArray {
			return ArrayScanner(typ)
		}
	}
	return valueScanners[kind]
}

func ptrScannerFunc(typ reflect.Type) ScannerFunc {
	scanner := Scanner(typ.Elem())
	return func(v reflect.Value, b []byte) error {
		if scanner == nil {
			return fmt.Errorf("pg: Scan(unsupported %s)", v.Type())
		}
		if b == nil {
			if v.IsNil() {
				return nil
			}
			if !v.CanSet() {
				return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
			}
			v.Set(reflect.Zero(v.Type()))
			return nil
		}
		if v.IsNil() {
			if !v.CanSet() {
				return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
			}
			v.Set(reflect.New(v.Type().Elem()))
		}
		return scanner(v.Elem(), b)
	}
}

func scanIfaceValue(v reflect.Value, b []byte) error {
	if v.IsNil() {
		return scanJSONValue(v, b)
	}
	return ScanValue(v.Elem(), b)
}

func ScanValue(v reflect.Value, b []byte) error {
	if !v.IsValid() {
		return errors.New("pg: Scan(nil)")
	}

	scanner := Scanner(v.Type())
	if scanner != nil {
		return scanner(v, b)
	}

	if v.Kind() == reflect.Interface {
		return errors.New("pg: Scan(nil)")
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
	v.SetBool(len(b) == 1 && (b[0] == 't' || b[0] == '1'))
	return nil
}

func scanIntValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	if b == nil {
		v.SetInt(0)
		return nil
	}
	n, err := internal.ParseInt(b, 10, 64)
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func scanUintValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	if b == nil {
		v.SetUint(0)
		return nil
	}
	n, err := internal.ParseUint(b, 10, 64)
	if err != nil {
		return err
	}
	v.SetUint(n)
	return nil
}

func scanFloatValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	if b == nil {
		v.SetFloat(0)
		return nil
	}
	n, err := internal.ParseFloat(b, 64)
	if err != nil {
		return err
	}
	v.SetFloat(n)
	return nil
}

func scanStringValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	v.SetString(string(b))
	return nil
}

func scanJSONValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	if b == nil {
		v.Set(reflect.New(v.Type()).Elem())
		return nil
	}
	return json.Unmarshal(b, v.Addr().Interface())
}

var zeroTimeValue = reflect.ValueOf(time.Time{})

func scanTimeValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	if b == nil {
		v.Set(zeroTimeValue)
		return nil
	}
	tm, err := ParseTime(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(tm))
	return nil
}

func scanIPValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	if b == nil {
		return nil
	}
	ip := net.ParseIP(internal.BytesToString(b))
	if ip == nil {
		return fmt.Errorf("pg: invalid ip=%q", b)
	}
	v.Set(reflect.ValueOf(ip))
	return nil
}

var zeroIPNetValue = reflect.ValueOf(net.IPNet{})

func scanIPNetValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	if b == nil {
		v.Set(zeroIPNetValue)
		return nil
	}
	_, ipnet, err := net.ParseCIDR(internal.BytesToString(b))
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(*ipnet))
	return nil
}

func scanBytesValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	if b == nil {
		v.SetBytes(nil)
		return nil
	}
	bs, err := ScanBytes(b)
	if err != nil {
		return err
	}
	v.SetBytes(bs)
	return nil
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
	if !v.CanAddr() {
		return fmt.Errorf("pg: Scan(nonsettable %s)", v.Type())
	}
	return scanSQLScanner(v.Addr().Interface().(sql.Scanner), b)
}
