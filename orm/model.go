package orm

import (
	"database/sql"
	"errors"
	"reflect"
	"time"

	"gopkg.in/pg.v4/types"
)

var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()

type Model interface {
	Collection
	ColumnScanner
}

func NewModel(values ...interface{}) (Model, error) {
	if len(values) > 1 {
		return Scan(values...), nil
	}

	v0 := values[0]
	if v0, ok := v0.(sql.Scanner); ok {
		return Scan(v0), nil
	}

	v := reflect.ValueOf(v0)
	if !v.IsValid() {
		return nil, errors.New("pg: NewModel(nil)")
	}
	v = reflect.Indirect(v)

	switch v.Kind() {
	case reflect.Struct:
		return newStructTableModel(v)
	case reflect.Slice:
		elType := indirectType(v.Type().Elem())
		if elType.Kind() == reflect.Struct && elType != timeType {
			return &sliceTableModel{
				structTableModel: structTableModel{
					table: Tables.Get(elType),
					root:  v,
				},
				slice: v,
			}, nil
		} else {
			return &sliceModel{
				slice: v,
				scan:  types.Scanner(elType),
			}, nil
		}
	}

	return Scan(v0), nil
}
