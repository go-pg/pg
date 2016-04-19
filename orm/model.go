package orm

import (
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

func NewModel(vi interface{}) (Model, error) {
	v := reflect.ValueOf(vi)
	if !v.IsValid() {
		return nil, errors.New("pg: NewModel(nil)")
	}
	v = reflect.Indirect(v)

	if v.Kind() == reflect.Slice {
		elType := indirectType(v.Type().Elem())
		if elType == timeType || elType.Kind() != reflect.Struct {
			return &simpleModel{
				slice: v,
				scan:  types.Scanner(elType),
			}, nil
		}
	}

	return newTableModelValue(v)
}
