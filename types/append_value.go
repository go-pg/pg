package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

var (
	appenderType = reflect.TypeOf(new(QueryAppender)).Elem()
)

var (
	stringSliceType  = reflect.TypeOf([]string(nil))
	intSliceType     = reflect.TypeOf([]int(nil))
	int64SliceType   = reflect.TypeOf([]int64(nil))
	float64SliceType = reflect.TypeOf([]float64(nil))
)

type valueAppender func([]byte, reflect.Value, bool) []byte

var valueAppenders = [...]valueAppender{
	reflect.Bool:          appendBoolValue,
	reflect.Int:           appendIntValue,
	reflect.Int8:          appendIntValue,
	reflect.Int16:         appendIntValue,
	reflect.Int32:         appendIntValue,
	reflect.Int64:         appendIntValue,
	reflect.Uint:          appendUintValue,
	reflect.Uint8:         appendUintValue,
	reflect.Uint16:        appendUintValue,
	reflect.Uint32:        appendUintValue,
	reflect.Uint64:        appendUintValue,
	reflect.Uintptr:       nil,
	reflect.Float32:       appendFloatValue,
	reflect.Float64:       appendFloatValue,
	reflect.Complex64:     nil,
	reflect.Complex128:    nil,
	reflect.Array:         nil,
	reflect.Chan:          nil,
	reflect.Func:          nil,
	reflect.Interface:     nil,
	reflect.Map:           nil,
	reflect.Ptr:           nil,
	reflect.Slice:         appendSliceValue,
	reflect.String:        AppendStringValue,
	reflect.Struct:        appendStructValue,
	reflect.UnsafePointer: nil,
}

func Appender(typ reflect.Type) valueAppender {
	switch typ {
	case timeType:
		return appendTimeValue
	}

	if typ.Implements(appenderType) {
		return appendAppenderValue
	}

	if typ.Implements(driverValuerType) {
		return appendDriverValuerValue
	}

	kind := typ.Kind()
	if appender := valueAppenders[kind]; appender != nil {
		return appender
	}

	return appendIfaceValue
}

func appendIfaceValue(dst []byte, v reflect.Value, quote bool) []byte {
	return Append(dst, v.Interface(), quote)
}

func appendBoolValue(b []byte, v reflect.Value, _ bool) []byte {
	return appendBool(b, v.Bool())
}

func appendIntValue(b []byte, v reflect.Value, _ bool) []byte {
	return strconv.AppendInt(b, v.Int(), 10)
}

func appendUintValue(b []byte, v reflect.Value, _ bool) []byte {
	return strconv.AppendUint(b, v.Uint(), 10)
}

func appendFloatValue(b []byte, v reflect.Value, _ bool) []byte {
	return appendFloat(b, v.Float())
}

func appendSliceValue(b []byte, v reflect.Value, quote bool) []byte {
	elemType := v.Type().Elem()
	switch elemType.Kind() {
	case reflect.Uint8:
		return appendBytes(b, v.Bytes(), quote)
	case reflect.String:
		ss := v.Convert(stringSliceType).Interface().([]string)
		return AppendStringSlice(b, ss, quote)
	case reflect.Int:
		ints := v.Convert(intSliceType).Interface().([]int)
		return appendIntSlice(b, ints, quote)
	case reflect.Int64:
		ints := v.Convert(int64SliceType).Interface().([]int64)
		return appendInt64Slice(b, ints, quote)
	case reflect.Float64:
		floats := v.Convert(float64SliceType).Interface().([]float64)
		return appendFloat64Slice(b, floats, quote)
	}
	panic(fmt.Errorf("pg: Decode(unsupported %s)", v.Type()))
}

func AppendStringValue(b []byte, v reflect.Value, quote bool) []byte {
	return AppendString(b, v.String(), quote)
}

func appendStructValue(b []byte, v reflect.Value, quote bool) []byte {
	switch v.Type() {
	case timeType:
		return appendTimeValue(b, v, quote)
	}
	bytes, err := json.Marshal(v.Interface())
	if err != nil {
		panic(err)
	}
	return AppendStringBytes(b, bytes, quote)
}

func appendTimeValue(b []byte, v reflect.Value, quote bool) []byte {
	tm := v.Interface().(time.Time)
	return appendTime(b, tm, quote)
}

func appendAppenderValue(b []byte, v reflect.Value, quote bool) []byte {
	return v.Interface().(QueryAppender).AppendQuery(b)
}

func appendDriverValuerValue(b []byte, v reflect.Value, quote bool) []byte {
	return appendDriverValuer(b, v.Interface().(driver.Valuer), quote)
}
