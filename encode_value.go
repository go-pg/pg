package pg

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type valueAppender func([]byte, reflect.Value) []byte

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
	reflect.Slice:         nil,
	reflect.String:        appendStringValue,
	reflect.Struct:        nil,
	reflect.UnsafePointer: nil,
}

func appendBoolValue(dst []byte, v reflect.Value) []byte {
	return appendBool(dst, v.Bool())
}

func appendIntValue(dst []byte, v reflect.Value) []byte {
	return strconv.AppendInt(dst, v.Int(), 10)
}

func appendUintValue(dst []byte, v reflect.Value) []byte {
	return strconv.AppendUint(dst, v.Uint(), 10)
}

func appendFloatValue(dst []byte, v reflect.Value) []byte {
	return appendFloat(dst, v.Float())
}

func appendStringValue(dst []byte, v reflect.Value) []byte {
	return appendString(dst, v.String())
}

func appendStructValue(dst []byte, v reflect.Value) []byte {
	switch v.Type() {
	case timeType:
		return appendTimeValue(dst, v)
	}
	panic(fmt.Sprintf("pg: unsupported src type: %s", v))
}

func appendTimeValue(dst []byte, v reflect.Value) []byte {
	dst = append(dst, '\'')
	dst = appendTime(dst, v.Interface().(time.Time))
	dst = append(dst, '\'')
	return dst
}

func appendAppenderValue(dst []byte, v reflect.Value) []byte {
	return v.Interface().(Appender).Append(dst)
}

func appendDriverValuerValue(dst []byte, v reflect.Value) []byte {
	return appendDriverValue(dst, v.Interface().(driver.Valuer))
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
