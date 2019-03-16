package types

import (
	"reflect"
	"strconv"
)

var stringType = reflect.TypeOf((*string)(nil)).Elem()
var sliceStringType = reflect.TypeOf([]string(nil))

var intType = reflect.TypeOf((*int)(nil)).Elem()
var sliceIntType = reflect.TypeOf([]int(nil))

var int64Type = reflect.TypeOf((*int64)(nil)).Elem()
var sliceInt64Type = reflect.TypeOf([]int64(nil))

var float64Type = reflect.TypeOf((*float64)(nil)).Elem()
var sliceFloat64Type = reflect.TypeOf([]float64(nil))

func ArrayAppender(typ reflect.Type) AppenderFunc {
	kind := typ.Kind()
	if kind == reflect.Ptr {
		typ = typ.Elem()
		kind = typ.Kind()
	}

	switch kind {
	case reflect.Slice, reflect.Array:
		// ok:
	default:
		return nil
	}

	elemType := typ.Elem()

	if kind == reflect.Slice {
		switch elemType {
		case stringType:
			return appendSliceStringValue
		case intType:
			return appendSliceIntValue
		case int64Type:
			return appendSliceInt64Value
		case float64Type:
			return appendSliceFloat64Value
		}
	}

	appendElem := appender(elemType, true)
	return func(b []byte, v reflect.Value, quote int) []byte {
		kind := v.Kind()
		switch kind {
		case reflect.Ptr, reflect.Slice:
			if v.IsNil() {
				return AppendNull(b, quote)
			}
		}

		if kind == reflect.Ptr {
			v = v.Elem()
		}

		if quote == 1 {
			b = append(b, '\'')
		}

		b = append(b, '{')
		for i := 0; i < v.Len(); i++ {
			elem := v.Index(i)
			b = appendElem(b, elem, 2)
			b = append(b, ',')
		}
		if v.Len() > 0 {
			b[len(b)-1] = '}' // Replace trailing comma.
		} else {
			b = append(b, '}')
		}

		if quote == 1 {
			b = append(b, '\'')
		}

		return b
	}
}

func appendSliceStringValue(b []byte, v reflect.Value, quote int) []byte {
	ss := v.Convert(sliceStringType).Interface().([]string)
	return appendSliceString(b, ss, quote)
}

func appendSliceString(b []byte, ss []string, quote int) []byte {
	if ss == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, s := range ss {
		b = AppendString(b, s, 2)
		b = append(b, ',')
	}
	if len(ss) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func appendSliceIntValue(b []byte, v reflect.Value, quote int) []byte {
	ints := v.Convert(sliceIntType).Interface().([]int)
	return appendSliceInt(b, ints, quote)
}

func appendSliceInt(b []byte, ints []int, quote int) []byte {
	if ints == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, n := range ints {
		b = strconv.AppendInt(b, int64(n), 10)
		b = append(b, ',')
	}
	if len(ints) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func appendSliceInt64Value(b []byte, v reflect.Value, quote int) []byte {
	ints := v.Convert(sliceInt64Type).Interface().([]int64)
	return appendSliceInt64(b, ints, quote)
}

func appendSliceInt64(b []byte, ints []int64, quote int) []byte {
	if ints == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, n := range ints {
		b = strconv.AppendInt(b, n, 10)
		b = append(b, ',')
	}
	if len(ints) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func appendSliceFloat64Value(b []byte, v reflect.Value, quote int) []byte {
	floats := v.Convert(sliceFloat64Type).Interface().([]float64)
	return appendSliceFloat64(b, floats, quote)
}

func appendSliceFloat64(b []byte, floats []float64, quote int) []byte {
	if floats == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, n := range floats {
		b = appendFloat(b, n, 2)
		b = append(b, ',')
	}
	if len(floats) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}
