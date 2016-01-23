package orm

import (
	"fmt"
	"reflect"
)

type sliceCollection struct {
	v reflect.Value // reflect.Slice
}

var _ Collection = (*sliceCollection)(nil)

func (coll *sliceCollection) newValue() reflect.Value {
	switch coll.v.Type().Elem().Kind() {
	case reflect.Ptr:
		elem := reflect.New(coll.v.Type().Elem().Elem())
		coll.v.Set(reflect.Append(coll.v, elem))
		return elem.Elem()
	case reflect.Struct:
		elem := reflect.New(coll.v.Type().Elem()).Elem()
		coll.v.Set(reflect.Append(coll.v, elem))
		elem = coll.v.Index(coll.v.Len() - 1)
		return elem
	default:
		panic("not reached")
	}
}

func (coll *sliceCollection) NextModel() interface{} {
	model, _ := NewModel(coll.newValue())
	return model
}

func newCollection(vi interface{}) (*sliceCollection, error) {
	v := reflect.ValueOf(vi)
	if !v.IsValid() {
		return nil, fmt.Errorf("pg: Decode(nil)")
	}
	if v.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("pg: Decode(nonsettable %T)", vi)
	}

	return newCollectionValue(v.Elem())
}

func newCollectionValue(v reflect.Value) (*sliceCollection, error) {
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("pg: Decode(unsupported %s)", v.Type())
	}

	elem := v.Type().Elem()
	switch elem.Kind() {
	case reflect.Struct:
		return &sliceCollection{v}, nil
	case reflect.Ptr:
		if elem.Elem().Kind() != reflect.Struct {
			return nil, fmt.Errorf("pg: Decode(unsupported %s)", v.Type())
		}
		return &sliceCollection{v}, nil
	default:
		return nil, fmt.Errorf("pg: Decode(unsupported %s)", v.Type())
	}
}
