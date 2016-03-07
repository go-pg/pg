package orm

import (
	"reflect"

	"gopkg.in/pg.v4/types"
)

func indirectType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func indirectNew(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	return v
}

func sliceNextElemValue(v reflect.Value) reflect.Value {
	if v.Type().Elem().Kind() == reflect.Ptr {
		elem := reflect.New(v.Type().Elem().Elem())
		v.Set(reflect.Append(v, elem))
		return elem.Elem()
	}

	elem := reflect.New(v.Type().Elem()).Elem()
	v.Set(reflect.Append(v, elem))
	elem = v.Index(v.Len() - 1)
	return elem
}

func columns(prefix string, fields []*Field) []byte {
	var b []byte
	for i, f := range fields {
		b = types.AppendField(b, prefix+f.SQLName, true)
		if i != len(fields)-1 {
			b = append(b, ", "...)
		}
	}
	return b
}

func values(v reflect.Value, path []string, fields ...*Field) []byte {
	b := walk(nil, v, path, fields)
	if len(b) > 0 {
		b = b[:len(b)-2] // trim ", "
	}
	return b
}

func walk(b []byte, v reflect.Value, path []string, fields []*Field) []byte {
	v = reflect.Indirect(v)
	if v.Kind() == reflect.Slice {
		return appendSliceField(b, v, path, fields)
	} else {
		return appendStructField(b, v, path, fields)
	}
}

func appendSliceField(b []byte, slice reflect.Value, path []string, fields []*Field) []byte {
	for i := 0; i < slice.Len(); i++ {
		b = appendStructField(b, slice.Index(i), path, fields)
	}
	return b
}

func appendStructField(b []byte, strct reflect.Value, path []string, fields []*Field) []byte {
	if len(path) > 0 {
		strct = strct.FieldByName(path[0])
		b = walk(b, strct, path[1:], fields)
		return b
	}

	b = append(b, '(')
	for i, field := range fields {
		b = field.AppendValue(b, strct, true)
		if i != len(fields)-1 {
			b = append(b, ", "...)
		}
	}
	b = append(b, "), "...)

	return b
}

func appendFieldValue(b []byte, v reflect.Value, fields []*Field) []byte {
	for i, f := range fields {
		b = types.AppendField(b, f.SQLName, true)
		b = append(b, " = "...)
		b = f.AppendValue(b, v, true)
		if i != len(fields)-1 {
			b = append(b, " AND "...)
		}
	}
	return b
}

func appendReturning(b []byte, v reflect.Value, fields []*Field) []byte {
	var hasReturning bool
	for i, f := range fields {
		if !f.IsEmpty(v) {
			continue
		}
		if !hasReturning {
			b = append(b, " RETURNING "...)
			hasReturning = true
		}
		b = types.AppendField(b, f.SQLName, true)
		if i != len(fields)-1 {
			b = append(b, ", "...)
		}
	}
	return b
}

func equal(vals []reflect.Value, strct reflect.Value, fields []*Field) bool {
	for i, f := range fields {
		if !f.Equal(strct, vals[i]) {
			return false
		}
	}
	return true
}
