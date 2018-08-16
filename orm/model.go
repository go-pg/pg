package orm

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/go-pg/pg/types"
)

type useQueryOne interface {
	useQueryOne() bool
}

type HooklessModel interface {
	// Init is responsible to initialize/reset model state.
	// It is called only once no matter how many rows
	// were returned by database.
	Init() error

	// NewModel returns ColumnScanner that is used to scan columns
	// from the current row. It is called once for every row.
	NewModel() ColumnScanner

	// AddModel adds ColumnScanner created by NewModel to the Collection.
	AddModel(ColumnScanner) error

	ColumnScanner
}

type Model interface {
	HooklessModel

	AfterQuery(DB) error

	BeforeSelectQuery(DB, *Query) (*Query, error)
	AfterSelect(DB) error

	BeforeInsert(DB) error
	AfterInsert(DB) error

	BeforeUpdate(DB) error
	AfterUpdate(DB) error

	BeforeDelete(DB) error
	AfterDelete(DB) error
}

func NewModel(values ...interface{}) (Model, error) {
	if len(values) > 1 {
		return Scan(values...), nil
	}

	v0 := values[0]
	switch v0 := v0.(type) {
	case Model:
		return v0, nil
	case HooklessModel:
		return newModelWithHookStubs(v0), nil
	case sql.Scanner:
		return Scan(v0), nil
	}

	v := reflect.ValueOf(v0)
	if !v.IsValid() {
		return nil, errors.New("pg: Model(nil)")
	}
	if v.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("pg: Model(non-pointer %T)", v0)
	}
	v = v.Elem()

	switch v.Kind() {
	case reflect.Struct:
		return newStructTableModelValue(v), nil
	case reflect.Slice:
		typ := v.Type()
		structType := indirectType(typ.Elem())
		if structType.Kind() == reflect.Struct && structType != timeType {
			m := sliceTableModel{
				structTableModel: structTableModel{
					table: GetTable(structType),
					root:  v,
				},
				slice: v,
			}
			m.init(typ)
			return &m, nil
		} else {
			return &sliceModel{
				slice: v,
				scan:  types.Scanner(structType),
			}, nil
		}
	}

	return Scan(v0), nil
}

type modelWithHookStubs struct {
	hookStubs
	HooklessModel
}

func newModelWithHookStubs(m HooklessModel) Model {
	return modelWithHookStubs{
		HooklessModel: m,
	}
}
