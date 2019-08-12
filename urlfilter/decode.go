package urlfilter

import (
	"reflect"
	"strings"

	"github.com/go-pg/pg/v9/internal/structfilter"
)

// Decode decodes url values into the struct.
func Decode(strct interface{}, values Values) error {
	v := reflect.Indirect(reflect.ValueOf(strct))
	meta := structfilter.GetStruct(v.Type())

	for name, values := range values {
		name = strings.TrimPrefix(name, ":")
		name = strings.TrimSuffix(name, "[]")

		field := meta.Field(name)
		if field != nil && !field.NoDecode() {
			err := field.Scan(field.Value(v), values)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
