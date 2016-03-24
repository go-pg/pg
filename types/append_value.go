package types

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strconv"
	"time"
)

var (
	appenderType = reflect.TypeOf(new(ValueAppender)).Elem()
)

type valueAppender func([]byte, reflect.Value, int) []byte

var valueAppenders []valueAppender

func init() {
	valueAppenders = []valueAppender{
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

func Appender(typ reflect.Type) valueAppender {
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

	if kind == reflect.Slice && typ.Elem().Kind() == reflect.Uint8 {
		return appendBytesValue
	}

	return valueAppenders[kind]
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

func appendIfaceValue(dst []byte, v reflect.Value, quote int) []byte {
	return Append(dst, v.Interface(), quote)
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
		panic(err)
	}
	return AppendJSONB(b, bytes, quote)
}

func appendTimeValue(b []byte, v reflect.Value, quote int) []byte {
	tm := v.Interface().(time.Time)
	return AppendTime(b, tm, quote)
}

func appendAppenderValue(b []byte, v reflect.Value, quote int) []byte {
	b, err := v.Interface().(ValueAppender).AppendValue(b, quote)
	if err != nil {
		panic(err)
	}
	return b
}

func appendDriverValuerValue(b []byte, v reflect.Value, quote int) []byte {
	return appendDriverValuer(b, v.Interface().(driver.Valuer), quote)
}
