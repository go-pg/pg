package urlfilter

import (
	"reflect"

	"github.com/go-pg/pg/v9/internal/structfilter"
)

// Decode decodes url values into the struct.
func Decode(strct interface{}, values Values) error {
	v := reflect.Indirect(reflect.ValueOf(strct))
	filter := structfilter.GetStruct(v.Type())
	for name, values := range values {
		err := filter.Decode(v, name, values)
		if err != nil {
			return err
		}
	}
	return nil
}
