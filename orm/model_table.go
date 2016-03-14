package orm

import (
	"errors"
	"fmt"
	"reflect"
)

type TableModel interface {
	Table() *Table

	Model
	AppendParam([]byte, string) ([]byte, error)

	Join(string) (string, error)
	GetJoin(string) (*Join, bool)
	GetJoins() []Join
	AddJoin(Join) *Join

	Kind() reflect.Kind
	Root() reflect.Value
	Path() []string
	Bind(reflect.Value)
	Value() reflect.Value

	scanColumn(int, string, []byte) (bool, error)
}

func NewTableModel(v interface{}) (TableModel, error) {
	switch v := (v).(type) {
	case TableModel:
		return v, nil
	case reflect.Value:
		return newTableModelValue(v)
	default:
		return newTableModelValue(reflect.ValueOf(v))
	}
}

func newTableModelValue(v reflect.Value) (TableModel, error) {
	if !v.IsValid() {
		return nil, errors.New("pg: NewModel(nil)")
	}
	v = reflect.Indirect(v)

	if v.Kind() == reflect.Struct {
		return &StructModel{
			table: Tables.Get(v.Type()),
			root:  v,
			strct: v,
		}, nil
	}

	if v.Kind() == reflect.Slice {
		elType := indirectType(v.Type().Elem())
		if elType.Kind() == reflect.Struct {
			return &SliceModel{
				StructModel: StructModel{
					table: Tables.Get(elType),
					root:  v,
				},
				slice: v,
			}, nil
		}
	}

	return nil, fmt.Errorf("pg: NewModel(unsupported %s)", v.Type())
}

func NewTableModelPath(root reflect.Value, path []string, table *Table) (TableModel, error) {
	v := fieldByPath(root, path)
	v = reflect.Indirect(v)

	if v.Kind() == reflect.Struct {
		return &StructModel{
			table: Tables.Get(v.Type()),
			root:  root,
			path:  path,
		}, nil
	}

	if v.Kind() == reflect.Slice {
		elType := indirectType(v.Type().Elem())
		if elType.Kind() == reflect.Struct {
			return &SliceModel{
				StructModel: StructModel{
					table: Tables.Get(elType),
					root:  root,
					path:  path,
				},
			}, nil
		}
	}

	return nil, fmt.Errorf("pg: NewTableModelPath(path %s on %s)", path, root.Type())
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
