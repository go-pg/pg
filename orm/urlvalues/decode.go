package urlvalues

import (
	"reflect"

	"github.com/go-pg/pg/internal/struct_filter"
)

func Decode(value interface{}, values Values) error {
	strct := reflect.Indirect(reflect.ValueOf(value))
	meta := struct_filter.GetStruct(strct.Type())

	for name, values := range values {
		field := meta.Field(name)
		if field != nil {
			err := field.Scan(field.Value(strct), values)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
