package pg

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strconv"
	"time"
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
	reflect.String:        appendStringValue,
	reflect.Struct:        appendStructValue,
	reflect.UnsafePointer: nil,
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
		return appendStringSlice(b, ss, quote)
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
	panic(errorf("pg: Decode(unsupported %s)", v.Type()))
}

func appendStringValue(b []byte, v reflect.Value, quote bool) []byte {
	return appendString(b, v.String(), quote)
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
	return appendStringBytes(b, bytes, quote)
}

func appendTimeValue(b []byte, v reflect.Value, quote bool) []byte {
	tm := v.Interface().(time.Time)
	return appendTime(b, tm, quote)
}

func appendAppenderValue(b []byte, v reflect.Value, quote bool) []byte {
	if quote {
		return v.Interface().(QueryAppender).AppendQuery(b)
	} else {
		return v.Interface().(RawQueryAppender).AppendRawQuery(b)
	}
}

func appendDriverValuerValue(b []byte, v reflect.Value, quote bool) []byte {
	return appendDriverValuer(b, v.Interface().(driver.Valuer), quote)
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}
