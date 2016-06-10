package orm

import (
	"errors"
	"fmt"
	"reflect"
)

type tableModel interface {
	Model

	Table() *Table
	AppendParam([]byte, string) ([]byte, bool)

	Join(string) *join
	GetJoin(string) *join
	GetJoins() []join
	AddJoin(join) *join

	Root() reflect.Value
	Path() []int
	Bind(reflect.Value)
	Value() reflect.Value

	scanColumn(int, string, []byte) (bool, error)
}

func newTableModel(v interface{}) (tableModel, error) {
	switch v := v.(type) {
	case tableModel:
		return v, nil
	case reflect.Value:
		return newTableModelValue(v)
	default:
		vv := reflect.ValueOf(v)
		if !vv.IsValid() {
			return nil, errors.New("pg: Model(nil)")
		}
		if vv.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("pg: Model(non-pointer %T)", v)
		}
		return newTableModelValue(vv.Elem())
	}
}

func newTableModelValue(v reflect.Value) (tableModel, error) {
	if !v.IsValid() {
		return nil, errors.New("pg: Model(nil)")
	}
	v = reflect.Indirect(v)

	switch v.Kind() {
	case reflect.Struct:
		return newStructTableModel(v)
	case reflect.Slice:
		elType := indirectType(v.Type().Elem())
		if elType.Kind() == reflect.Interface && v.Len() > 0 {
			elType = reflect.Indirect(v.Index(0).Elem()).Type()
		}
		if elType.Kind() == reflect.Struct {
			return &sliceTableModel{
				structTableModel: structTableModel{
					table: Tables.Get(elType),
					root:  v,
				},
				slice: v,
			}, nil
		}
	}

	return nil, fmt.Errorf("pg: Model(unsupported %s)", v.Type())
}

func newTableModelPath(root reflect.Value, path []int, table *Table) (tableModel, error) {
	v := fieldByPath(root, path)
	v = reflect.Indirect(v)

	if v.Kind() == reflect.Struct {
		return &structTableModel{
			table: Tables.Get(v.Type()),
			root:  root,
			path:  path,
		}, nil
	}

	if v.Kind() == reflect.Slice {
		elType := indirectType(v.Type().Elem())
		if elType.Kind() == reflect.Struct {
			return &sliceTableModel{
				structTableModel: structTableModel{
					table: Tables.Get(elType),
					root:  root,
					path:  path,
				},
			}, nil
		}
	}

	return nil, fmt.Errorf("pg: newTableModelPath(path %s on %s)", path, root.Type())
}

func fieldByPath(v reflect.Value, path []int) reflect.Value {
	for _, index := range path {
		if v.Kind() == reflect.Slice {
			v = reflect.Zero(v.Type().Elem())
		}

		v = v.Field(index)
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				v = reflect.New(v.Type().Elem())
			}
			v = v.Elem()
		}
	}
	return v
}
