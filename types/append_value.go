package types

import (
	"database/sql/driver"
	"encoding/json"
	"net"
	"reflect"
	"strconv"
	"time"

	"github.com/go-pg/pg/internal"
)

var driverValuerType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
var appenderType = reflect.TypeOf((*ValueAppender)(nil)).Elem()

type AppenderFunc func([]byte, reflect.Value, int) []byte

var valueAppenders []AppenderFunc

//nolint
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
		reflect.Array:         appendJSONValue,
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
	switch typ {
	case timeType:
		return appendTimeValue
	case ipType:
		return appendIPValue
	case ipNetType:
		return appendIPNetValue
	case jsonRawMessageType:
		return appendJSONRawMessageValue
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
	case reflect.Array:
		if typ.Elem().Kind() == reflect.Uint8 {
			return appendArrayBytesValue
		}
	}
	return valueAppenders[kind]
}

func ptrAppenderFunc(typ reflect.Type) AppenderFunc {
	appender := Appender(typ.Elem())
	return func(b []byte, v reflect.Value, flags int) []byte {
		if v.IsNil() {
			return AppendNull(b, flags)
		}
		return appender(b, v.Elem(), flags)
	}
}

func appendValue(b []byte, v reflect.Value, flags int) []byte {
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return AppendNull(b, flags)
	}
	appender := Appender(v.Type())
	return appender(b, v, flags)
}

func appendIfaceValue(b []byte, v reflect.Value, flags int) []byte {
	return Append(b, v.Interface(), flags)
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

func appendFloatValue(b []byte, v reflect.Value, flags int) []byte {
	return appendFloat(b, v.Float(), flags)
}

func appendBytesValue(b []byte, v reflect.Value, flags int) []byte {
	return AppendBytes(b, v.Bytes(), flags)
}

func appendArrayBytesValue(b []byte, v reflect.Value, flags int) []byte {
	return AppendBytes(b, v.Slice(0, v.Len()).Bytes(), flags)
}

func appendStringValue(b []byte, v reflect.Value, flags int) []byte {
	return AppendString(b, v.String(), flags)
}

func appendStructValue(b []byte, v reflect.Value, flags int) []byte {
	if v.Type() == timeType {
		return appendTimeValue(b, v, flags)
	}
	return appendJSONValue(b, v, flags)
}

func appendJSONValue(b []byte, v reflect.Value, flags int) []byte {
	bytes, err := json.Marshal(v.Interface())
	if err != nil {
		return AppendError(b, err)
	}
	return AppendJSONB(b, bytes, flags)
}

func appendTimeValue(b []byte, v reflect.Value, flags int) []byte {
	tm := v.Interface().(time.Time)
	return AppendTime(b, tm, flags)
}

func appendIPValue(b []byte, v reflect.Value, flags int) []byte {
	ip := v.Interface().(net.IP)
	return AppendString(b, ip.String(), flags)
}

func appendIPNetValue(b []byte, v reflect.Value, flags int) []byte {
	ipnet := v.Interface().(net.IPNet)
	return AppendString(b, ipnet.String(), flags)
}

func appendJSONRawMessageValue(b []byte, v reflect.Value, flags int) []byte {
	return AppendString(b, internal.BytesToString(v.Bytes()), flags)
}

func appendAppenderValue(b []byte, v reflect.Value, flags int) []byte {
	return appendAppender(b, v.Interface().(ValueAppender), flags)
}

func appendDriverValuerValue(b []byte, v reflect.Value, flags int) []byte {
	return appendDriverValuer(b, v.Interface().(driver.Valuer), flags)
}
