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

type valueDecoder func(reflect.Value, []byte) error

var valueDecoders []valueDecoder

func init() {
	valueDecoders = []valueDecoder{
		reflect.Bool:          decodeBoolValue,
		reflect.Int:           decodeIntValue,
		reflect.Int8:          decodeIntValue,
		reflect.Int16:         decodeIntValue,
		reflect.Int32:         decodeIntValue,
		reflect.Int64:         decodeIntValue,
		reflect.Uint:          decodeUintValue,
		reflect.Uint8:         decodeUintValue,
		reflect.Uint16:        decodeUintValue,
		reflect.Uint32:        decodeUintValue,
		reflect.Uint64:        decodeUintValue,
		reflect.Uintptr:       nil,
		reflect.Float32:       decodeFloatValue,
		reflect.Float64:       decodeFloatValue,
		reflect.Complex64:     nil,
		reflect.Complex128:    nil,
		reflect.Array:         nil,
		reflect.Chan:          nil,
		reflect.Func:          nil,
		reflect.Interface:     decodeInterfaceValue,
		reflect.Map:           decodeJSONValue,
		reflect.Ptr:           decodePtrValue,
		reflect.Slice:         decodeJSONValue,
		reflect.String:        decodeStringValue,
		reflect.Struct:        decodeJSONValue,
		reflect.UnsafePointer: nil,
	}
}

func Decoder(typ reflect.Type) valueDecoder {
	if typ == timeType {
		return decodeTimeValue
	}

	if reflect.PtrTo(typ).Implements(scannerType) {
		return decodeScannerAddrValue
	}

	if typ.Implements(scannerType) {
		return decodeScannerValue
	}

	kind := typ.Kind()

	if kind == reflect.Slice && typ.Elem().Kind() == reflect.Uint8 {
		return decodeBytesValue
	}

	if dec := valueDecoders[kind]; dec != nil {
		return dec
	}

	return nil
}

func DecodeValue(v reflect.Value, b []byte) error {
	if !v.IsValid() {
		return fmt.Errorf("pg: Decode(nil)")
	}

	if b == nil {
		return decodeNullValue(v)
	}

	decoder := Decoder(v.Type())
	if decoder != nil {
		return decoder(v, b)
	}

	if v.Kind() == reflect.Interface {
		return fmt.Errorf("pg: Decode(nil)")
	}
	return fmt.Errorf("pg: Decode(unsupported %s)", v.Type())
}

func decodeBoolValue(v reflect.Value, b []byte) error {
	if !v.CanSet() {
		return fmt.Errorf("pg: Decode(nonsettable %s)", v.Type())
	}
	v.SetBool(len(b) == 1 && b[0] == 't')
	return nil
}

func decodeIntValue(v reflect.Value, b []byte) error {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func decodeUintValue(v reflect.Value, b []byte) error {
	n, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return err
	}
	v.SetUint(n)
	return nil
}

func decodeFloatValue(v reflect.Value, b []byte) error {
	n, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return err
	}
	v.SetFloat(n)
	return nil
}

func decodeStringValue(v reflect.Value, b []byte) error {
	v.SetString(string(b))
	return nil
}

func decodeJSONValue(v reflect.Value, b []byte) error {
	return json.Unmarshal(b, v.Addr().Interface())
}

func decodeTimeValue(v reflect.Value, b []byte) error {
	tm, err := ParseTime(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(tm))
	return nil
}

func decodePtrValue(v reflect.Value, b []byte) error {
	if v.IsNil() {
		if !v.CanSet() {
			return fmt.Errorf("pg: Decode(nonsettable %s)", v.Type())
		}
		vv := reflect.New(v.Type().Elem())
		v.Set(vv)
	}
	return DecodeValue(v.Elem(), b)
}

func decodeBytesValue(v reflect.Value, b []byte) error {
	bs, err := decodeBytes(b)
	if err != nil {
		return err
	}
	v.SetBytes(bs)
	return nil
}

func decodeInterfaceValue(v reflect.Value, b []byte) error {
	if v.IsNil() {
		return fmt.Errorf("pg: Decode(nil)")
	}
	return DecodeValue(v.Elem(), b)
}

func decodeMapValue(v reflect.Value, b []byte) error {
	typ := v.Type()
	if typ.Key().Kind() == reflect.String && typ.Elem().Kind() == reflect.String {
		m, err := decodeStringStringMap(b)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(m))
		return nil
	}
	return fmt.Errorf("pg: Decode(unsupported %s)", v.Type())
}

func decodeNullValue(v reflect.Value) error {
	kind := v.Kind()
	switch kind {
	case reflect.Interface:
		return decodeNullValue(v.Elem())
	}
	if v.CanSet() {
		v.Set(reflect.Zero(v.Type()))
		return nil
	}
	if kind == reflect.Ptr {
		return decodeNullValue(v.Elem())
	}
	return nil
}

func decodeScannerValue(v reflect.Value, b []byte) error {
	if v.IsNil() {
		v.Set(reflect.New(v.Type().Elem()))
	}
	return decodeScanner(v.Interface().(sql.Scanner), b)
}

func decodeScannerAddrValue(v reflect.Value, b []byte) error {
	return decodeScanner(v.Addr().Interface().(sql.Scanner), b)
}
