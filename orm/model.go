package orm

import (
	"errors"
	"reflect"
	"time"

	"gopkg.in/pg.v4/types"
)

var (
	timePtrType = reflect.TypeOf((*time.Time)(nil))
	timeType    = timePtrType.Elem()
)

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
			return &sliceModel{
				slice:   v,
				decoder: types.Decoder(elType),
			}, nil
		}
	}

	return newTableModelValue(v)
}
