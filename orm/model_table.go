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
	Index() []int
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

func newTableModelIndex(root reflect.Value, index []int, table *Table) (tableModel, error) {
	typ := typeByIndex(root.Type(), index)

	if typ.Kind() == reflect.Struct {
		return &structTableModel{
			table: Tables.Get(typ),
			root:  root,
			index: index,
		}, nil
	}

	if typ.Kind() == reflect.Slice {
		elType := indirectType(typ.Elem())
		if elType.Kind() == reflect.Struct {
			return &sliceTableModel{
				structTableModel: structTableModel{
					table: Tables.Get(elType),
					root:  root,
					index: index,
				},
			}, nil
		}
	}

	return nil, fmt.Errorf("pg: NewModel(index %s on %s)", index, root.Type())
}
