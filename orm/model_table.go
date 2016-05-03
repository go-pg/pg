package orm

import (
	"errors"
	"fmt"
	"reflect"
)

type tableModel interface {
	Table() *Table

	Model

	Join(string) *Join
	GetJoin(string) *Join
	GetJoins() []Join
	AddJoin(Join) *Join

	Root() reflect.Value
	Path() []string
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
		return newTableModelValue(reflect.ValueOf(v))
	}
}

func newTableModelValue(v reflect.Value) (tableModel, error) {
	if !v.IsValid() {
		return nil, errors.New("pg: NewModel(nil)")
	}
	v = reflect.Indirect(v)

	switch v.Kind() {
	case reflect.Struct:
		return newStructTableModel(v)
	case reflect.Slice:
		elType := indirectType(v.Type().Elem())
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

	return nil, fmt.Errorf("pg: NewModel(unsupported %s)", v.Type())
}

func newTableModelPath(root reflect.Value, path []string, table *Table) (tableModel, error) {
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

func fieldByPath(v reflect.Value, path []string) reflect.Value {
	for _, name := range path {
		if v.Kind() == reflect.Slice {
			v = reflect.Zero(v.Type().Elem())
		}

		v = v.FieldByName(name)
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				v = reflect.New(v.Type().Elem())
			}
			v = v.Elem()
		}
	}
	return v
}
