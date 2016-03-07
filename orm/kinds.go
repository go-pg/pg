package orm

import (
	"fmt"
	"reflect"
)

type isEmptyFunc func(reflect.Value) bool
type equalFunc func(reflect.Value, reflect.Value) bool

func isEmptier(kind reflect.Kind) isEmptyFunc {
	switch kind {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return isEmptyLen
	case reflect.Bool:
		return isEmptyBool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return isEmptyInt
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return isEmptyUint
	case reflect.Float32, reflect.Float64:
		return isEmptyFloat
	case reflect.Interface, reflect.Ptr:
		return isEmptyNil
	}
	return isEmptyFalse
}

func isEmptyLen(v reflect.Value) bool {
	return v.Len() == 0
}

func isEmptyNil(v reflect.Value) bool {
	return v.IsNil()
}

func isEmptyBool(v reflect.Value) bool {
	return !v.Bool()
}

func isEmptyInt(v reflect.Value) bool {
	return v.Int() == 0
}

func isEmptyUint(v reflect.Value) bool {
	return v.Uint() == 0
}

func isEmptyFloat(v reflect.Value) bool {
	return v.Float() == 0
}

func isEmptyFalse(v reflect.Value) bool {
	return false
}

func equaler(kind reflect.Kind) equalFunc {
	switch kind {
	case reflect.String:
		return equalString
	case reflect.Bool:
		return equalBool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return equalInt
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return equalUint
	case reflect.Float32, reflect.Float64:
		return equalFloat
	}
	return equalUnsupported
}

func equalString(v1, v2 reflect.Value) bool {
	return v1.String() == v2.String()
}

func equalBool(v1, v2 reflect.Value) bool {
	return v1.Bool() == v2.Bool()
}

func equalInt(v1, v2 reflect.Value) bool {
	return v1.Int() == v2.Int()
}

func equalUint(v1, v2 reflect.Value) bool {
	return v1.Uint() == v2.Uint()
}

func equalFloat(v1, v2 reflect.Value) bool {
	return v1.Float() == v2.Float()
}

func equalUnsupported(v1, v2 reflect.Value) bool {
	panic(fmt.Errorf("equal is not supported for %s and %s", v1.Type(), v2.Type()))
}
