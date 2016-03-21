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

func values(v reflect.Value, path []string, fields []*Field) []byte {
	var b []byte
	walk(v, path, func(v reflect.Value) {
		b = append(b, '(')
		for i, field := range fields {
			b = field.AppendValue(b, v, true)
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

func modelId(b []byte, v reflect.Value, fields []*Field) []byte {
	for i, f := range fields {
		b = f.AppendValue(b, v, false)
		if i != len(fields)-1 {
			b = append(b, ',')
		}
	}
	return b
}

func modelIdMap(b []byte, m map[string]string, fields []*Field) []byte {
	for i, f := range fields {
		b = append(b, m[f.SQLName]...)
		if i != len(fields)-1 {
			b = append(b, ',')
		}
	}
	return b
}

func dstValues(model TableModel) map[string][]reflect.Value {
	path := model.Path()
	mp := make(map[string][]reflect.Value)
	b := make([]byte, 16)
	walk(model.Root(), path[:len(path)-1], func(v reflect.Value) {
		b = b[:0]
		id := string(modelId(b, v, model.Table().PKs))
		mp[id] = append(mp[id], v.FieldByName(path[len(path)-1]))
	})
	return mp
}
