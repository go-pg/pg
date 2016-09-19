package types

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strconv"
	"time"
)

var appenderType = reflect.TypeOf((*ValueAppender)(nil)).Elem()

type AppenderFunc func([]byte, reflect.Value, int) []byte

var valueAppenders []AppenderFunc

func init() {
	valueAppenders = []AppenderFunc{
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
		reflect.Interface:     appendIfaceValue,
		reflect.Map:           appendJSONValue,
		reflect.Ptr:           nil,
		reflect.Slice:         appendJSONValue,
		reflect.String:        appendStringValue,
		reflect.Struct:        appendStructValue,
		reflect.UnsafePointer: nil,
	}
}

func Appender(typ reflect.Type) AppenderFunc {
	return appender(typ, false)
}

func appender(typ reflect.Type, pgArray bool) AppenderFunc {
	if typ == timeType {
		return appendTimeValue
	}

	if typ.Implements(appenderType) {
		return appendAppenderValue
	}

	if typ.Implements(driverValuerType) {
		return appendDriverValuerValue
	}

	kind := typ.Kind()
	switch kind {
	case reflect.Ptr:
		return ptrAppenderFunc(typ)
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			return appendBytesValue
		}
		if pgArray {
			return ArrayAppender(typ)
		}
	}
	return valueAppenders[kind]
}

func ptrAppenderFunc(typ reflect.Type) AppenderFunc {
	appender := Appender(typ.Elem())
	return func(b []byte, v reflect.Value, quote int) []byte {
		if v.IsNil() {
			return AppendNull(b, quote)
		}
		return appender(b, v.Elem(), quote)
	}
}

func appendValue(b []byte, v reflect.Value, quote int) []byte {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return AppendNull(b, quote)
		}
		return appendValue(b, v.Elem(), quote)
	}

	appender := Appender(v.Type())
	return appender(b, v, quote)
}

func appendIfaceValue(b []byte, v reflect.Value, quote int) []byte {
	return Append(b, v.Interface(), quote)
}

func appendBoolValue(b []byte, v reflect.Value, _ int) []byte {
	return appendBool(b, v.Bool())
}

func appendIntValue(b []byte, v reflect.Value, _ int) []byte {
	return strconv.AppendInt(b, v.Int(), 10)
}

func appendUintValue(b []byte, v reflect.Value, _ int) []byte {
	return strconv.AppendUint(b, v.Uint(), 10)
}

func appendFloatValue(b []byte, v reflect.Value, _ int) []byte {
	return appendFloat(b, v.Float())
}

func appendBytesValue(b []byte, v reflect.Value, quote int) []byte {
	return appendBytes(b, v.Bytes(), quote)
}

func appendStringValue(b []byte, v reflect.Value, quote int) []byte {
	return AppendString(b, v.String(), quote)
}

func appendStructValue(b []byte, v reflect.Value, quote int) []byte {
	if v.Type() == timeType {
		return appendTimeValue(b, v, quote)
	}
	return appendJSONValue(b, v, quote)
}

func appendJSONValue(b []byte, v reflect.Value, quote int) []byte {
	bytes, err := json.Marshal(v.Interface())
	if err != nil {
		return AppendError(b, err)
	}
	return AppendJSONB(b, bytes, quote)
}

func appendTimeValue(b []byte, v reflect.Value, quote int) []byte {
	tm := v.Interface().(time.Time)
	return AppendTime(b, tm, quote)
}

func appendAppenderValue(b []byte, v reflect.Value, quote int) []byte {
	return appendAppender(b, v.Interface().(ValueAppender), quote)
}

func appendDriverValuerValue(b []byte, v reflect.Value, quote int) []byte {
	return appendDriverValuer(b, v.Interface().(driver.Valuer), quote)
}
