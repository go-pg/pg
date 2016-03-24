package types

import (
	"reflect"
	"strconv"
)

var (
	stringSliceType  = reflect.TypeOf([]string(nil))
	intSliceType     = reflect.TypeOf([]int(nil))
	int64SliceType   = reflect.TypeOf([]int64(nil))
	float64SliceType = reflect.TypeOf([]float64(nil))
)

var sliceAppenders = []AppenderFunc{
	reflect.Bool:          nil,
	reflect.Int:           appendIntSliceValue,
	reflect.Int8:          nil,
	reflect.Int16:         nil,
	reflect.Int32:         nil,
	reflect.Int64:         appendInt64SliceValue,
	reflect.Uint:          nil,
	reflect.Uint8:         nil,
	reflect.Uint16:        nil,
	reflect.Uint32:        nil,
	reflect.Uint64:        nil,
	reflect.Uintptr:       nil,
	reflect.Float32:       nil,
	reflect.Float64:       appendFloat64SliceValue,
	reflect.Complex64:     nil,
	reflect.Complex128:    nil,
	reflect.Array:         nil,
	reflect.Chan:          nil,
	reflect.Func:          nil,
	reflect.Interface:     nil,
	reflect.Map:           nil,
	reflect.Ptr:           nil,
	reflect.Slice:         nil,
	reflect.String:        appendStringSliceValue,
	reflect.Struct:        nil,
	reflect.UnsafePointer: nil,
}

func ArrayAppender(typ reflect.Type) AppenderFunc {
	elemType := typ.Elem()

	if appender := sliceAppenders[elemType.Kind()]; appender != nil {
		return appender
	}

	appendElem := Appender(elemType)
	return func(b []byte, v reflect.Value, quote int) []byte {
		if v.IsNil() {
			return AppendNull(b, quote)
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

func appendStringSliceValue(b []byte, v reflect.Value, quote int) []byte {
	ss := v.Convert(stringSliceType).Interface().([]string)
	return appendStringSlice(b, ss, quote)
}

func appendStringSlice(b []byte, ss []string, quote int) []byte {
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

func appendIntSliceValue(b []byte, v reflect.Value, quote int) []byte {
	ints := v.Convert(intSliceType).Interface().([]int)
	return appendIntSlice(b, ints, quote)
}

func appendIntSlice(b []byte, ints []int, quote int) []byte {
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

func appendInt64SliceValue(b []byte, v reflect.Value, quote int) []byte {
	ints := v.Convert(int64SliceType).Interface().([]int64)
	return appendInt64Slice(b, ints, quote)
}

func appendInt64Slice(b []byte, ints []int64, quote int) []byte {
	if ints == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, "{"...)
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

func appendFloat64SliceValue(b []byte, v reflect.Value, quote int) []byte {
	floats := v.Convert(float64SliceType).Interface().([]float64)
	return appendFloat64Slice(b, floats, quote)
}

func appendFloat64Slice(b []byte, floats []float64, quote int) []byte {
	if floats == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	b = append(b, "{"...)
	for _, n := range floats {
		b = appendFloat(b, n)
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
