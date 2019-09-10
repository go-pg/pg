package urlfilter

import (
	"reflect"
	"strings"

	"github.com/go-pg/pg/v9/internal/structfilter"
)

// Decode decodes url values into the struct.
func Decode(strct interface{}, values Values) error {
	v := reflect.Indirect(reflect.ValueOf(strct))
	filter := structfilter.GetStruct(v.Type())

	var maps map[string][]string
	for name, values := range values {
		if name, key, ok := mapKey(name); ok {
			if maps == nil {
				maps = make(map[string][]string)
			}
			maps[name] = append(maps[name], key, values[0])
			continue
		}

		err := filter.Decode(v, name, values)
		if err != nil {
			return err
		}
	}

	for name, values := range maps {
		err := filter.Decode(v, name, values)
		if err != nil {
			return nil
		}
	}

	return nil
}

func mapKey(s string) (name string, key string, ok bool) {
	ind := strings.IndexByte(s, '[')
	if ind == -1 || s[len(s)-1] != ']' {
		return "", "", false
	}
	key = s[ind+1 : len(s)-1]
	if key == "" {
		return "", "", false
	}
	name = s[:ind]
	return name, key, true
}
