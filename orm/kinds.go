package orm

import (
	"reflect"
	"time"
)

type isEmptyFunc func(reflect.Value) bool

func isEmptier(typ reflect.Type) isEmptyFunc {
	if typ == timeType {
		return isEmptyTime
	}
	switch typ.Kind() {
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

func isEmptyTime(v reflect.Value) bool {
	return v.Interface().(time.Time).IsZero()
}

func isEmptyFalse(v reflect.Value) bool {
	return false
}
