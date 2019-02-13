package orm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/go-pg/pg/types"
)

var errModelNil = errors.New("pg: Model(nil)")

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
}

type Model interface {
	HooklessModel

	AfterQuery(context.Context, DB) error

	BeforeSelectQuery(context.Context, DB, *Query) (*Query, error)
	AfterSelect(context.Context, DB) error

	BeforeInsert(context.Context, DB) error
	AfterInsert(context.Context, DB) error

	BeforeUpdate(context.Context, DB) error
	AfterUpdate(context.Context, DB) error

	BeforeDelete(context.Context, DB) error
	AfterDelete(context.Context, DB) error
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
	case types.ValueScanner, sql.Scanner:
		return Scan(v0), nil
	}

	v := reflect.ValueOf(v0)
	if !v.IsValid() {
		return nil, errModelNil
	}
	if v.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("pg: Model(non-pointer %T)", v0)
	}
	v = v.Elem()

	switch v.Kind() {
	case reflect.Struct:
		if v.Type() != timeType {
			return newStructTableModelValue(v), nil
		}
	case reflect.Slice:
		typ := v.Type()
		elemType := indirectType(typ.Elem())
		if elemType.Kind() == reflect.Struct && elemType != timeType {
			return newSliceTableModel(v, elemType), nil
		} else {
			return newSliceModel(v, elemType), nil
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
