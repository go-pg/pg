package types

import (
	"fmt"
	"reflect"
)

var mapStringStringType = reflect.TypeOf(map[string]string(nil))

func HstoreAppender(typ reflect.Type) AppenderFunc {
	if typ.Key() == stringType && typ.Elem() == stringType {
		return appendMapStringStringValue
	}
	return func(b []byte, v reflect.Value, quote int) []byte {
		err := fmt.Errorf("pg.Hstore(unsupported %s)", v.Type())
		return AppendError(b, err)
	}
}

func appendMapStringString(b []byte, m map[string]string, quote int) []byte {
	if m == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	for key, value := range m {
		b = AppendString(b, key, 2)
		b = append(b, '=', '>')
		b = AppendString(b, value, 2)
		b = append(b, ',')
	}
	if len(m) > 0 {
		b = b[:len(b)-1] // Strip trailing comma.
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func appendMapStringStringValue(b []byte, v reflect.Value, quote int) []byte {
	m := v.Convert(mapStringStringType).Interface().(map[string]string)
	return appendMapStringString(b, m, quote)
}
