package pg

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"time"

	"gopkg.in/pg.v3/pgutil"
)

func AppendQ(dst []byte, src string, params ...interface{}) ([]byte, error) {
	return formatQuery(dst, []byte(src), params)
}

func FormatQ(src string, params ...interface{}) (Q, error) {
	b, err := AppendQ(nil, src, params...)
	if err != nil {
		return "", err
	}
	return Q(b), nil
}

func MustFormatQ(src string, params ...interface{}) Q {
	q, err := FormatQ(src, params...)
	if err != nil {
		panic(err)
	}
	return q
}

func appendIface(dst []byte, srci interface{}) []byte {
	if srci == nil {
		return appendNull(dst)
	}

	switch src := srci.(type) {
	case bool:
		return appendBool(dst, src)
	case int8:
		return strconv.AppendInt(dst, int64(src), 10)
	case int16:
		return strconv.AppendInt(dst, int64(src), 10)
	case int32:
		return strconv.AppendInt(dst, int64(src), 10)
	case int64:
		return strconv.AppendInt(dst, int64(src), 10)
	case int:
		return strconv.AppendInt(dst, int64(src), 10)
	case uint8:
		return strconv.AppendUint(dst, uint64(src), 10)
	case uint16:
		return strconv.AppendUint(dst, uint64(src), 10)
	case uint32:
		return strconv.AppendUint(dst, uint64(src), 10)
	case uint64:
		return strconv.AppendUint(dst, src, 10)
	case uint:
		return strconv.AppendUint(dst, uint64(src), 10)
	case float32:
		return appendFloat(dst, float64(src))
	case float64:
		return appendFloat(dst, src)
	case string:
		return appendString(dst, src)
	case time.Time:
		dst = append(dst, '\'')
		dst = pgutil.AppendTime(dst, src)
		dst = append(dst, '\'')
		return dst
	case []byte:
		dst = append(dst, '\'')
		dst = appendBytes(dst, src)
		dst = append(dst, '\'')
		return dst
	case []string:
		dst = append(dst, '\'')
		dst = appendStringSlice(dst, src, false)
		dst = append(dst, '\'')
		return dst
	case []int:
		dst = append(dst, '\'')
		dst = appendIntSlice(dst, src)
		dst = append(dst, '\'')
		return dst
	case []int64:
		dst = append(dst, '\'')
		dst = appendInt64Slice(dst, src)
		dst = append(dst, '\'')
		return dst
	case map[string]string:
		dst = append(dst, '\'')
		dst = appendStringStringMap(dst, src, false)
		dst = append(dst, '\'')
		return dst
	case QueryAppender:
		return src.AppendQuery(dst)
	case driver.Valuer:
		return appendDriverValuer(dst, src)
	default:
		return appendValue(dst, reflect.ValueOf(srci))
	}
}

func appendValue(dst []byte, v reflect.Value) []byte {
	switch kind := v.Kind(); kind {
	case reflect.Ptr:
		if v.IsNil() {
			return appendNull(dst)
		}
		return appendValue(dst, v.Elem())
	default:
		if appender := valueAppenders[kind]; appender != nil {
			return appender(dst, v)
		}
	}
	panic(fmt.Sprintf("pg: unsupported src type: %s", v))
}

// Returns nil when src is NULL.
func appendIfaceRaw(dst []byte, srci interface{}) []byte {
	if srci == nil {
		return nil
	}

	switch src := srci.(type) {
	case bool:
		return appendBool(dst, src)
	case int8:
		return strconv.AppendInt(dst, int64(src), 10)
	case int16:
		return strconv.AppendInt(dst, int64(src), 10)
	case int32:
		return strconv.AppendInt(dst, int64(src), 10)
	case int64:
		return strconv.AppendInt(dst, int64(src), 10)
	case int:
		return strconv.AppendInt(dst, int64(src), 10)
	case uint8:
		return strconv.AppendInt(dst, int64(src), 10)
	case uint16:
		return strconv.AppendInt(dst, int64(src), 10)
	case uint32:
		return strconv.AppendInt(dst, int64(src), 10)
	case uint64:
		return strconv.AppendInt(dst, int64(src), 10)
	case uint:
		return strconv.AppendInt(dst, int64(src), 10)
	case float32:
		return appendFloat(dst, float64(src))
	case float64:
		return appendFloat(dst, src)
	case string:
		return appendStringRaw(dst, src)
	case time.Time:
		return pgutil.AppendTime(dst, src)
	case []byte:
		return appendBytes(dst, src)
	case []string:
		return appendStringSlice(dst, src, true)
	case []int:
		return appendIntSlice(dst, src)
	case []int64:
		return appendInt64Slice(dst, src)
	case map[string]string:
		return appendStringStringMap(dst, src, true)
	case RawQueryAppender:
		return src.AppendRawQuery(dst)
	case driver.Valuer:
		return appendDriverValueRaw(dst, src)
	default:
		return appendValueRaw(dst, reflect.ValueOf(srci))
	}
}

func appendValueRaw(dst []byte, v reflect.Value) []byte {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return nil
		}
		return appendValueRaw(dst, v.Elem())
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		return strconv.AppendInt(dst, v.Int(), 10)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return strconv.AppendUint(dst, v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return appendFloat(dst, v.Float())
	case reflect.String:
		return appendStringRaw(dst, v.String())
	case reflect.Struct:
		if v.Type() == timeType {
			return pgutil.AppendTime(dst, v.Interface().(time.Time))
		}
	}
	panic(fmt.Sprintf("pg: unsupported src type: %s", v))
}

func appendNull(dst []byte) []byte {
	return append(dst, "NULL"...)
}

func appendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, "TRUE"...)
	}
	return append(dst, "FALSE"...)
}

func appendFloat(dst []byte, v float64) []byte {
	return strconv.AppendFloat(dst, v, 'f', -1, 64)
}

func appendString(dst []byte, src string) []byte {
	dst = append(dst, '\'')
	for _, c := range []byte(src) {
		switch c {
		case '\'':
			dst = append(dst, "''"...)
		case '\000':
			continue
		default:
			dst = append(dst, c)
		}
	}
	dst = append(dst, '\'')
	return dst
}

func appendStringRaw(dst []byte, src string) []byte {
	for _, c := range []byte(src) {
		if c != '\000' {
			dst = append(dst, c)
		}
	}
	return dst
}

func appendSubstring(dst []byte, src string, raw bool) []byte {
	dst = append(dst, '"')
	for _, c := range []byte(src) {
		switch c {
		case '\'':
			if raw {
				dst = append(dst, '\'')
			} else {
				dst = append(dst, '\'', '\'')
			}
		case '\000':
			continue
		case '\\':
			dst = append(dst, '\\', '\\')
		case '"':
			dst = append(dst, '\\', '"')
		default:
			dst = append(dst, c)
		}
	}
	dst = append(dst, '"')
	return dst
}

func appendBytes(dst []byte, v []byte) []byte {
	tmp := make([]byte, hex.EncodedLen(len(v)))
	hex.Encode(tmp, v)

	dst = append(dst, "\\x"...)
	dst = append(dst, tmp...)
	return dst
}

func appendStringStringMap(dst []byte, v map[string]string, raw bool) []byte {
	if len(v) == 0 {
		return dst
	}

	for key, value := range v {
		dst = appendSubstring(dst, key, raw)
		dst = append(dst, '=', '>')
		dst = appendSubstring(dst, value, raw)
		dst = append(dst, ',')
	}
	dst = dst[:len(dst)-1] // Strip trailing comma.
	return dst
}

func appendStringSlice(dst []byte, v []string, raw bool) []byte {
	if len(v) == 0 {
		return append(dst, "{}"...)
	}

	dst = append(dst, '{')
	for _, s := range v {
		dst = appendSubstring(dst, s, raw)
		dst = append(dst, ',')
	}
	dst[len(dst)-1] = '}' // Replace trailing comma.
	return dst
}

func appendIntSlice(dst []byte, v []int) []byte {
	if len(v) == 0 {
		return append(dst, "{}"...)
	}

	dst = append(dst, '{')
	for _, n := range v {
		dst = strconv.AppendInt(dst, int64(n), 10)
		dst = append(dst, ',')
	}
	dst[len(dst)-1] = '}' // Replace trailing comma.
	return dst
}

func appendInt64Slice(dst []byte, v []int64) []byte {
	if len(v) == 0 {
		return append(dst, "{}"...)
	}

	dst = append(dst, "{"...)
	for _, n := range v {
		dst = strconv.AppendInt(dst, n, 10)
		dst = append(dst, ',')
	}
	dst[len(dst)-1] = '}' // Replace trailing comma.
	return dst
}

func appendDriverValuer(dst []byte, v driver.Valuer) []byte {
	value, err := v.Value()
	if err != nil {
		log.Printf("%#v value failed: %s", v, err)
		return appendNull(dst)
	}
	return appendIface(dst, value)
}

func appendDriverValueRaw(dst []byte, v driver.Valuer) []byte {
	value, err := v.Value()
	if err != nil {
		log.Printf("%#v value failed: %s", v, err)
		return nil
	}
	return appendIfaceRaw(dst, value)
}

//------------------------------------------------------------------------------

func formatQuery(dst, src []byte, params []interface{}) ([]byte, error) {
	if len(params) == 0 {
		return append(dst, src...), nil
	}

	var structptr, structv reflect.Value
	var fields map[string]*pgValue
	var methods map[string]*pgValue
	var paramInd int

	p := &parser{b: src}

	for p.Valid() {
		ch := p.Next()
		if ch == '\\' {
			if p.Peek() == '?' {
				p.SkipNext()
				dst = append(dst, '?')
				continue
			}
		} else if ch != '?' {
			dst = append(dst, ch)
			continue
		}

		name := p.ReadName()
		if name != "" {
			// Lazily initialize named params.
			if fields == nil {
				if len(params) == 0 {
					return nil, errorf("pg: expected at least one parameter, got nothing")
				}
				structptr = reflect.ValueOf(params[len(params)-1])
				params = params[:len(params)-1]
				if structptr.Kind() == reflect.Ptr {
					structv = structptr.Elem()
				} else {
					structv = structptr
				}
				if structv.Kind() != reflect.Struct {
					return nil, errorf("pg: expected struct, got %s", structv.Kind())
				}
				fields = structs.Fields(structv.Type())
				methods = structs.Methods(structptr.Type())
			}

			if field, ok := fields[name]; ok {
				dst = field.AppendValue(dst, structv)
				continue
			} else if field, ok := methods[name]; ok {
				dst = field.AppendValue(dst, structptr)
				continue
			} else {
				return nil, errorf("pg: cannot map %q", name)
			}
		} else {
			if paramInd >= len(params) {
				return nil, errorf(
					"pg: expected at least %d parameters, got %d",
					paramInd+1, len(params),
				)
			}

			dst = appendIface(dst, params[paramInd])
			paramInd++
		}
	}

	if paramInd < len(params) {
		return nil, errorf("pg: expected %d parameters, got %d", paramInd, len(params))
	}

	return dst, nil
}
