package orm

import (
	"errors"
	"fmt"
	"reflect"
)

type tableModel interface {
	Model

	Table() *Table
	Relation() *Relation
	AppendParam([]byte, QueryFormatter, string) ([]byte, bool)

	Join(string, func(*Query) (*Query, error)) *join
	GetJoin(string) *join
	GetJoins() []join
	AddJoin(join) *join

	Root() reflect.Value
	Index() []int
	ParentIndex() []int
	Mount(reflect.Value)
	Kind() reflect.Kind
	Value() reflect.Value

	setDeletedAt()
	scanColumn(int, string, []byte) (bool, error)
}

func newTableModel(value interface{}) (tableModel, error) {
	if value, ok := value.(tableModel); ok {
		return value, nil
	}

	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return nil, errors.New("pg: Model(nil)")
	}
	if v.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("pg: Model(non-pointer %T)", value)
	}

	if v.IsNil() {
		typ := v.Type().Elem()
		if typ.Kind() == reflect.Struct {
			return newStructTableModelType(typ), nil
		}
		return nil, errors.New("pg: Model(nil)")
	}

	return newTableModelValue(v.Elem())
}

func newTableModelValue(v reflect.Value) (tableModel, error) {
	switch v.Kind() {
	case reflect.Struct:
		return newStructTableModelValue(v), nil
	case reflect.Slice:
		structType := sliceElemType(v)
		if structType.Kind() == reflect.Struct {
			m := sliceTableModel{
				structTableModel: structTableModel{
					table: GetTable(structType),
					root:  v,
				},
				slice: v,
			}
			m.init(v.Type())
			return &m, nil
		}
	}

	return nil, fmt.Errorf("pg: Model(unsupported %s)", v.Type())
}

func newTableModelIndex(root reflect.Value, index []int, rel *Relation) (tableModel, error) {
	typ := typeByIndex(root.Type(), index)

	if typ.Kind() == reflect.Struct {
		return &structTableModel{
			table: GetTable(typ),
			rel:   rel,

			root:  root,
			index: index,
		}, nil
	}

	if typ.Kind() == reflect.Slice {
		structType := indirectType(typ.Elem())
		if structType.Kind() == reflect.Struct {
			m := sliceTableModel{
				structTableModel: structTableModel{
					table: GetTable(structType),
					rel:   rel,

					root:  root,
					index: index,
				},
			}
			m.init(typ)
			return &m, nil
		}
	}

	return nil, fmt.Errorf("pg: NewModel(%s)", typ)
}
