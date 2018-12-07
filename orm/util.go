package orm

import (
	"reflect"

	"github.com/go-pg/pg/types"
)

func indirect(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Interface:
		return indirect(v.Elem())
	case reflect.Ptr:
		return v.Elem()
	default:
		return v
	}
}

func indirectType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func sliceElemType(v reflect.Value) reflect.Type {
	elemType := v.Type().Elem()
	if elemType.Kind() == reflect.Interface && v.Len() > 0 {
		return indirect(v.Index(0).Elem()).Type()
	} else {
		return indirectType(elemType)
	}
}

func typeByIndex(t reflect.Type, index []int) reflect.Type {
	for _, x := range index {
		switch t.Kind() {
		case reflect.Ptr:
			t = t.Elem()
		case reflect.Slice:
			t = indirectType(t.Elem())
		}
		t = t.Field(x).Type
	}
	return indirectType(t)
}

func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for i, x := range index {
		if i > 0 {
			v = indirectNew(v)
		}
		v = v.Field(x)
	}
	return v
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

func walk(v reflect.Value, index []int, fn func(reflect.Value)) {
	v = reflect.Indirect(v)
	switch v.Kind() {
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			visitField(v.Index(i), index, fn)
		}
	default:
		visitField(v, index, fn)
	}
}

func visitField(v reflect.Value, index []int, fn func(reflect.Value)) {
	v = reflect.Indirect(v)
	if len(index) > 0 {
		v = v.Field(index[0])
		if v.Kind() == reflect.Ptr && v.IsNil() {
			return
		}
		walk(v, index[1:], fn)
	} else {
		fn(v)
	}
}

func dstValues(model TableModel, fields []*Field) map[string][]reflect.Value {
	mp := make(map[string][]reflect.Value)
	var id []byte
	walk(model.Root(), model.ParentIndex(), func(v reflect.Value) {
		id = modelId(id[:0], v, fields)
		mp[string(id)] = append(mp[string(id)], v.FieldByIndex(model.Relation().Field.Index))
	})
	return mp
}

func modelId(b []byte, v reflect.Value, fields []*Field) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, ',')
		}
		b = f.AppendValue(b, v, 0)
	}
	return b
}

func appendColumns(b []byte, table types.Q, fields []*Field) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		if len(table) > 0 {
			b = append(b, table...)
			b = append(b, '.')
		}
		b = types.AppendField(b, f.SQLName, 1)
	}
	return b
}
