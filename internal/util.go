package internal

import (
	"reflect"
	"strings"
)

func MakeSliceNextElemFunc(v reflect.Value) func() reflect.Value {
	if v.Kind() == reflect.Array {
		var pos int
		return func() reflect.Value {
			v := v.Index(pos)
			pos++
			return v
		}
	}

	elemType := v.Type().Elem()

	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		return func() reflect.Value {
			if v.Len() < v.Cap() {
				v.Set(v.Slice(0, v.Len()+1))
				elem := v.Index(v.Len() - 1)
				if elem.IsNil() {
					elem.Set(reflect.New(elemType))
				}
				return elem.Elem()
			}

			elem := reflect.New(elemType)
			v.Set(reflect.Append(v, elem))
			return elem.Elem()
		}
	}

	zero := reflect.Zero(elemType)
	return func() reflect.Value {
		if v.Len() < v.Cap() {
			v.Set(v.Slice(0, v.Len()+1))
			return v.Index(v.Len() - 1)
		}

		v.Set(reflect.Append(v, zero))
		return v.Index(v.Len() - 1)
	}
}

func QuoteTableName(s string) string {
	if isPostgresKeyword(s) {
		return `"` + s + `"`
	}
	return s
}

func isPostgresKeyword(s string) bool {
	switch strings.ToLower(s) {
	case "user", "group", "constraint", "limit",
		"member", "placing", "references", "table":
		return true
	default:
		return false
	}
}
