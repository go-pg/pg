package orm

import "reflect"

var isZeroerType = reflect.TypeOf((*isZeroer)(nil)).Elem()

type isZeroer interface {
	IsZero() bool
}

func isEmptyFunc(typ reflect.Type) func(reflect.Value) bool {
	if typ.Implements(isZeroerType) {
		return isEmptyZero
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

func isEmptyZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return true
		}
	}
	return v.Interface().(isZeroer).IsZero()
}

func isEmptyFalse(v reflect.Value) bool {
	return false
}
