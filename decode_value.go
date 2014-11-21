package pg

import (
	"reflect"
	"strconv"
	"time"
)

var (
	timePtrType = reflect.TypeOf((*time.Time)(nil))
	timeType    = timePtrType.Elem()
)

type valueDecoder func(reflect.Value, []byte) error

var valueDecoders = [...]valueDecoder{
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
	reflect.Interface:     nil,
	reflect.Map:           decodeMapValue,
	reflect.Ptr:           nil,
	reflect.Slice:         decodeSliceValue,
	reflect.String:        decodeStringValue,
	reflect.Struct:        decodeStructValue,
	reflect.UnsafePointer: nil,
}

func DecodeValue(dst reflect.Value, f []byte) error {
	if !dst.IsValid() {
		return decodeError(dst)
	}

	if f == nil {
		return decodeNullValue(dst)
	}

	kind := dst.Kind()
	if kind == reflect.Ptr && dst.IsNil() && dst.CanSet() {
		dst.Set(reflect.New(dst.Type().Elem()))
	}

	if err, ok := tryDecodeInterfaces(dst.Interface(), f); ok {
		return err
	}

	if kind == reflect.Interface || kind == reflect.Ptr {
		v := dst.Elem()
		if !v.IsValid() {
			return decodeError(dst)
		}
		return DecodeValue(v, f)
	}

	if !dst.CanSet() {
		return decodeError(dst)
	}

	if decoder := valueDecoders[kind]; decoder != nil {
		return decoder(dst, f)
	}
	return errorf("pg: unsupported dst: %s", dst.Type())
}

func decodeBoolValue(v reflect.Value, b []byte) error {
	v.SetBool(len(b) == 1 && b[0] == 't')
	return nil
}

func decodeIntValue(v reflect.Value, b []byte) error {
	if b == nil {
		return decodeNullValue(v)
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	v.SetInt(n)
	return nil
}

func decodeUintValue(v reflect.Value, b []byte) error {
	if b == nil {
		return decodeNullValue(v)
	}
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

func decodeStructValue(v reflect.Value, b []byte) error {
	if v.Type() == timeType {
		return decodeTimeValue(v, b)
	}
	return errorf("pg: unsupported dst: %s", v.Type())
}

func decodeTimeValue(v reflect.Value, b []byte) error {
	tm, err := decodeTime(b)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(tm))
	return nil
}

func decodeSliceValue(dst reflect.Value, f []byte) error {
	elemType := dst.Type().Elem()
	switch elemType.Kind() {
	case reflect.Uint8:
		b, err := decodeBytes(f)
		if err != nil {
			return err
		}
		dst.SetBytes(b)
		return nil
	case reflect.String:
		s, err := decodeStringSlice(f)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
		return nil
	case reflect.Int:
		s, err := decodeIntSlice(f)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
		return nil
	case reflect.Int64:
		s, err := decodeInt64Slice(f)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
		return nil
	}
	return errorf("pg: unsupported dst: %s", dst.Type())
}

func decodeMapValue(dst reflect.Value, f []byte) error {
	typ := dst.Type()
	if typ.Key().Kind() == reflect.String && typ.Elem().Kind() == reflect.String {
		m, err := decodeStringStringMap(f)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(m))
		return nil
	}
	return errorf("pg: unsupported dst: %s", dst.Type())
}

func decodeNullValue(v reflect.Value) error {
	kind := v.Kind()
	if kind == reflect.Interface {
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
