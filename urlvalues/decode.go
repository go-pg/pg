package urlvalues

import (
	"reflect"
	"strings"

	"github.com/go-pg/pg/internal/structfilter"
)

// Decode decodes url values into the struct.
func Decode(strct interface{}, values Values) error {
	v := reflect.Indirect(reflect.ValueOf(strct))
	meta := structfilter.GetStruct(v.Type())

	for name, values := range values {
		if strings.HasSuffix(name, "[]") {
			name = name[:len(name)-2]
		}

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
