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

func sliceNextElem(v reflect.Value) reflect.Value {
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

func columns(table types.Q, prefix string, fields []*Field) []byte {
	var b []byte
	for i, f := range fields {
		if table != nil {
			b, _ = table.AppendValue(b, 1)
			b = append(b, '.')
		}
		b = types.AppendField(b, prefix+f.SQLName, 1)
		if i != len(fields)-1 {
			b = append(b, ", "...)
		}
	}
	return b
}

func values(v reflect.Value, path []string, fields []*Field) []byte {
	var b []byte
	walk(v, path, func(v reflect.Value) {
		b = append(b, '(')
		for i, field := range fields {
			b = field.AppendValue(b, v, 1)
			if i != len(fields)-1 {
				b = append(b, ", "...)
			}
		}
		b = append(b, "), "...)
	})
	if len(b) > 0 {
		b = b[:len(b)-2] // trim ", "
	}
	return b
}

func walk(v reflect.Value, path []string, fn func(reflect.Value)) {
	v = reflect.Indirect(v)
	if v.Kind() == reflect.Slice {
		walkSlice(v, path, fn)
	} else {
		visitStruct(v, path, fn)
	}
}

func walkSlice(slice reflect.Value, path []string, fn func(reflect.Value)) {
	for i := 0; i < slice.Len(); i++ {
		visitStruct(slice.Index(i), path, fn)
	}
}

func visitStruct(strct reflect.Value, path []string, fn func(reflect.Value)) {
	if len(path) > 0 {
		strct = strct.FieldByName(path[0])
		walk(strct, path[1:], fn)
	} else {
		fn(strct)
	}
}

func appendFieldValue(b []byte, v reflect.Value, fields []*Field) []byte {
	for i, f := range fields {
		b = append(b, f.ColName...)
		b = append(b, " = "...)
		b = f.AppendValue(b, v, 1)
		if i != len(fields)-1 {
			b = append(b, " AND "...)
		}
	}
	return b
}

func appendReturning(b []byte, v reflect.Value, fields []*Field) []byte {
	var hasReturning bool
	for _, f := range fields {
		if !f.IsEmpty(v) {
			continue
		}
		if !hasReturning {
			b = append(b, " RETURNING "...)
			hasReturning = true
		}
		b = append(b, f.ColName...)
		b = append(b, ", "...)
	}
	if hasReturning {
		b = b[:len(b)-2]
	}
	return b
}

func modelId(b []byte, v reflect.Value, fields []*Field) []byte {
	for i, f := range fields {
		b = f.AppendValue(b, v, 0)
		if i != len(fields)-1 {
			b = append(b, ',')
		}
	}
	return b
}

func modelIdMap(b []byte, m map[string]string, prefix string, fields []*Field) []byte {
	for i, f := range fields {
		b = append(b, m[prefix+f.SQLName]...)
		if i != len(fields)-1 {
			b = append(b, ',')
		}
	}
	return b
}

func dstValues(root reflect.Value, path []string, fields []*Field) map[string][]reflect.Value {
	mp := make(map[string][]reflect.Value)
	b := make([]byte, 16)
	walk(root, path[:len(path)-1], func(v reflect.Value) {
		b = b[:0]
		id := string(modelId(b, v, fields))
		mp[id] = append(mp[id], v.FieldByName(path[len(path)-1]))
	})
	return mp
}

func appendSep(b []byte, sep string) []byte {
	if len(b) > 0 {
		b = append(b, sep...)
	}
	return b
}

func col(s string) types.Q {
	return types.Q(types.AppendField(nil, s, 1))
}
