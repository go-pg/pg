package internal

import "reflect"

func SliceNextElem(v reflect.Value) reflect.Value {
	if v.Len() < v.Cap() {
		v.Set(v.Slice(0, v.Len()+1))
		return v.Index(v.Len() - 1)
	}

	elemType := v.Type().Elem()

	if elemType.Kind() == reflect.Ptr {
		elem := reflect.New(elemType.Elem())
		v.Set(reflect.Append(v, elem))
		return elem.Elem()
	}

	v.Set(reflect.Append(v, reflect.Zero(elemType)))
	return v.Index(v.Len() - 1)
}
