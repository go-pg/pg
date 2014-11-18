package pg

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/golang/glog"
)

const (
	dateFormat         = "2006-01-02"
	timeFormat         = "15:04:05.999999999"
	timestampFormat    = "2006-01-02 15:04:05.999999999"
	timestamptzFormat  = "2006-01-02 15:04:05.999999999-07:00"
	timestamptzFormat2 = "2006-01-02 15:04:05.999999999-07"
	timestamptzFormat3 = "2006-01-02 15:04:05.999999999-07:00:00"
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

func appendRawString(dst []byte, src string) []byte {
	for _, c := range []byte(src) {
		if c != '\000' {
			dst = append(dst, c)
		}
	}
	return dst
}

func appendBytes(dst []byte, src []byte) []byte {
	tmp := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(tmp, src)

	dst = append(dst, "'\\x"...)
	dst = append(dst, tmp...)
	dst = append(dst, '\'')
	return dst
}

func appendSubstring(dst []byte, src string) []byte {
	dst = append(dst, '"')
	for _, c := range []byte(src) {
		switch c {
		case '\'':
			dst = append(dst, "''"...)
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

func appendRawSubstring(dst []byte, src string) []byte {
	dst = append(dst, '"')
	for _, c := range []byte(src) {
		switch c {
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

func appendTime(dst []byte, tm time.Time) []byte {
	dst = append(dst, '\'')
	dst = append(dst, tm.Local().Format(timestamptzFormat)...)
	dst = append(dst, '\'')
	return dst
}

func appendRawTime(dst []byte, tm time.Time) []byte {
	return append(dst, tm.Local().Format(timestamptzFormat)...)
}

func appendIface(dst []byte, srci interface{}) []byte {
	if srci == nil {
		return append(dst, "NULL"...)
	}

	switch src := srci.(type) {
	case bool:
		if src {
			return append(dst, "TRUE"...)
		}
		return append(dst, "FALSE"...)
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
		return strconv.AppendFloat(dst, float64(src), 'f', -1, 32)
	case float64:
		return strconv.AppendFloat(dst, src, 'f', -1, 64)
	case string:
		return appendString(dst, src)
	case time.Time:
		return appendTime(dst, src)
	case []byte:
		return appendBytes(dst, src)
	case []string:
		if len(src) == 0 {
			return append(dst, "'{}'"...)
		}

		dst = append(dst, "'{"...)
		for _, s := range src {
			dst = appendSubstring(dst, s)
			dst = append(dst, ',')
		}
		dst[len(dst)-1] = '}'
		dst = append(dst, '\'')
		return dst
	case []int:
		if len(src) == 0 {
			return append(dst, "'{}'"...)
		}

		dst = append(dst, "'{"...)
		for _, n := range src {
			dst = strconv.AppendInt(dst, int64(n), 10)
			dst = append(dst, ',')
		}
		dst[len(dst)-1] = '}'
		dst = append(dst, '\'')
		return dst
	case []int64:
		if len(src) == 0 {
			return append(dst, "'{}'"...)
		}

		dst = append(dst, "'{"...)
		for _, n := range src {
			dst = strconv.AppendInt(dst, n, 10)
			dst = append(dst, ',')
		}
		dst[len(dst)-1] = '}'
		dst = append(dst, '\'')
		return dst
	case map[string]string:
		if len(src) == 0 {
			return append(dst, "''"...)
		}

		dst = append(dst, '\'')
		for key, value := range src {
			dst = appendSubstring(dst, key)
			dst = append(dst, '=', '>')
			dst = appendSubstring(dst, value)
			dst = append(dst, ',')
		}
		dst[len(dst)-1] = '\''
		return dst
	case Appender:
		return src.Append(dst)
	case driver.Valuer:
		v, err := src.Value()
		if err != nil {
			glog.Errorf("%#v value failed: %s", src, err)
			return append(dst, "NULL"...)
		}
		return appendIface(dst, v)
	default:
		return appendValue(dst, reflect.ValueOf(srci))
	}
}

func appendValue(dst []byte, v reflect.Value) []byte {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return append(dst, "NULL"...)
		}
		return appendValue(dst, v.Elem())
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		return strconv.AppendInt(dst, v.Int(), 10)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return strconv.AppendUint(dst, v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.AppendFloat(dst, v.Float(), 'f', -1, 64)
	case reflect.String:
		return appendString(dst, v.String())
	case reflect.Struct:
		if v.Type() == timeType {
			return appendTime(dst, v.Interface().(time.Time))
		}
	}
	panic(fmt.Sprintf("pg: unsupported src type: %s", v))
}

// Returns nil when src is NULL.
func appendRawIface(dst []byte, srci interface{}) []byte {
	if srci == nil {
		return nil
	}

	switch src := srci.(type) {
	case bool:
		if src {
			return append(dst, "TRUE"...)
		}
		return append(dst, "FALSE"...)
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
		return strconv.AppendFloat(dst, float64(src), 'f', -1, 32)
	case float64:
		return strconv.AppendFloat(dst, src, 'f', -1, 64)
	case string:
		return appendRawString(dst, src)
	case time.Time:
		return appendRawTime(dst, src)
	case []byte:
		tmp := make([]byte, hex.EncodedLen(len(src)))
		hex.Encode(tmp, src)

		dst = append(dst, "\\x"...)
		dst = append(dst, tmp...)
		return dst
	case []string:
		if len(src) == 0 {
			return append(dst, "{}"...)
		}

		dst = append(dst, "{"...)
		for _, s := range src {
			dst = appendRawSubstring(dst, s)
			dst = append(dst, ',')
		}
		dst[len(dst)-1] = '}'
		return dst
	case []int:
		if len(src) == 0 {
			return append(dst, "{}"...)
		}

		dst = append(dst, "{"...)
		for _, n := range src {
			dst = strconv.AppendInt(dst, int64(n), 10)
			dst = append(dst, ',')
		}
		dst[len(dst)-1] = '}'
		return dst
	case []int64:
		if len(src) == 0 {
			return append(dst, "{}"...)
		}

		dst = append(dst, "{"...)
		for _, n := range src {
			dst = strconv.AppendInt(dst, n, 10)
			dst = append(dst, ',')
		}
		dst[len(dst)-1] = '}'
		return dst
	case map[string]string:
		if len(src) == 0 {
			return dst
		}

		for key, value := range src {
			dst = appendRawSubstring(dst, key)
			dst = append(dst, '=', '>')
			dst = appendRawSubstring(dst, value)
			dst = append(dst, ',')
		}
		dst = dst[:len(dst)-1]
		return dst
	case RawAppender:
		return src.AppendRaw(dst)
	case driver.Valuer:
		v, err := src.Value()
		if err != nil {
			glog.Errorf("%#v value failed: %s", src, err)
			return nil
		}
		return appendRawIface(dst, v)
	default:
		return appendRawValue(dst, reflect.ValueOf(srci))
	}
}

func appendRawValue(dst []byte, v reflect.Value) []byte {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return nil
		}
		return appendRawValue(dst, v.Elem())
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		return strconv.AppendInt(dst, v.Int(), 10)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return strconv.AppendUint(dst, v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.AppendFloat(dst, v.Float(), 'f', -1, 64)
	case reflect.String:
		return appendRawString(dst, v.String())
	case reflect.Struct:
		if v.Type() == timeType {
			return appendRawTime(dst, v.Interface().(time.Time))
		}
	}
	panic(fmt.Sprintf("pg: unsupported src type: %s", v))
}

//------------------------------------------------------------------------------

func formatQuery(dst, src []byte, params []interface{}) ([]byte, error) {
	if len(params) == 0 {
		return append(dst, src...), nil
	}

	var structptr, structv reflect.Value
	var fields map[string][]int
	var methods map[string]int
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

		var value interface{}

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

			if indx, ok := fields[name]; ok {
				value = structv.FieldByIndex(indx).Interface()
			} else if indx, ok := methods[name]; ok {
				value = structptr.Method(indx).Call(nil)[0].Interface()
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

			value = params[paramInd]
			paramInd++
		}

		dst = appendIface(dst, value)
	}

	if paramInd < len(params) {
		return nil, errorf("pg: expected %d parameters, got %d", paramInd, len(params))
	}

	return dst, nil
}
