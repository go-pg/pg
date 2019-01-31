package orm

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/types"
)

type TableModel interface {
	Model

	IsNil() bool
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

	setSoftDeleteField()
	scanColumn(int, string, types.Reader, int) (bool, error)
}

func newTableModel(value interface{}) (TableModel, error) {
	if value, ok := value.(TableModel); ok {
		return value, nil
	}

	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return nil, errModelNil
	}
	if v.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("pg: Model(non-pointer %T)", value)
	}

	if v.IsNil() {
		typ := v.Type().Elem()
		if typ.Kind() == reflect.Struct {
			return newStructTableModel(GetTable(typ)), nil
		}
		return nil, errModelNil
	}

	return newTableModelValue(v.Elem())
}

func newTableModelValue(v reflect.Value) (TableModel, error) {
	switch v.Kind() {
	case reflect.Struct:
		return newStructTableModelValue(v), nil
	case reflect.Slice:
		elemType := sliceElemType(v)
		if elemType.Kind() == reflect.Struct {
			return newSliceTableModel(v, elemType), nil
		}
	}

	return nil, fmt.Errorf("pg: Model(unsupported %s)", v.Type())
}

func newTableModelIndex(root reflect.Value, index []int, rel *Relation) (TableModel, error) {
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
