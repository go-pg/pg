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

func appendIface(b []byte, vi interface{}, quote bool) []byte {
	switch v := vi.(type) {
	case nil:
		return appendNull(b, quote)
	case bool:
		return appendBool(b, v)
	case int8:
		return strconv.AppendInt(b, int64(v), 10)
	case int16:
		return strconv.AppendInt(b, int64(v), 10)
	case int32:
		return strconv.AppendInt(b, int64(v), 10)
	case int64:
		return strconv.AppendInt(b, int64(v), 10)
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case uint8:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint16:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint32:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint64:
		return strconv.AppendUint(b, v, 10)
	case uint:
		return strconv.AppendUint(b, uint64(v), 10)
	case float32:
		return appendFloat(b, float64(v))
	case float64:
		return appendFloat(b, v)
	case string:
		return appendString(b, v, quote)
	case time.Time:
		return appendTime(b, v, quote)
	case []byte:
		return appendBytes(b, v, quote)
	case []string:
		return appendStringSlice(b, v, quote)
	case []int:
		return appendIntSlice(b, v, quote)
	case []int64:
		return appendInt64Slice(b, v, quote)
	case []float64:
		return appendFloat64Slice(b, v, quote)
	case map[string]string:
		return appendStringStringMap(b, v, quote)
	case QueryAppender:
		return v.AppendQuery(b)
	case driver.Valuer:
		return appendDriverValuer(b, v, quote)
	default:
		return appendValue(b, reflect.ValueOf(vi), quote)
	}
}

func appendValue(b []byte, v reflect.Value, quote bool) []byte {
	switch kind := v.Kind(); kind {
	case reflect.Ptr:
		if v.IsNil() {
			return appendNull(b, quote)
		}
		return appendValue(b, v.Elem(), quote)
	default:
		if appender := valueAppenders[kind]; appender != nil {
			return appender(b, v, quote)
		}
	}
	panic(fmt.Sprintf("pg: Format(unsupported %s)", v.Type()))
}

func appendNull(b []byte, quote bool) []byte {
	if quote {
		return append(b, "NULL"...)
	} else {
		return nil
	}
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

func appendString(b []byte, s string, quote bool) []byte {
	return appendStringBytes(b, []byte(s), quote)
}

func appendStringBytes(b []byte, bytes []byte, quote bool) []byte {
	if quote {
		b = append(b, '\'')
	}

	for _, c := range bytes {
		switch c {
		case '\'':
			if quote {
				b = append(b, '\'', '\'')
			} else {
				b = append(b, '\'')
			}
		case '\000':
			continue
		default:
			b = append(b, c)
		}
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendSubstring(b []byte, src string, quote bool) []byte {
	b = append(b, '"')
	for _, c := range []byte(src) {
		switch c {
		case '\'':
			if quote {
				b = append(b, '\'', '\'')
			} else {
				b = append(b, '\'')
			}
		case '\000':
			continue
		case '\\':
			b = append(b, '\\', '\\')
		case '"':
			b = append(b, '\\', '"')
		default:
			b = append(b, c)
		}
	}
	b = append(b, '"')
	return b
}

func appendBytes(b []byte, bytes []byte, quote bool) []byte {
	if bytes == nil {
		return appendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	tmp := make([]byte, hex.EncodedLen(len(bytes)))
	hex.Encode(tmp, bytes)
	b = append(b, "\\x"...)
	b = append(b, tmp...)

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendStringStringMap(b []byte, m map[string]string, quote bool) []byte {
	if m == nil {
		return appendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	for key, value := range m {
		b = appendSubstring(b, key, quote)
		b = append(b, '=', '>')
		b = appendSubstring(b, value, quote)
		b = append(b, ',')
	}
	if len(m) > 0 {
		b = b[:len(b)-1] // Strip trailing comma.
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendStringSlice(b []byte, ss []string, quote bool) []byte {
	if ss == nil {
		return appendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, s := range ss {
		b = appendSubstring(b, s, quote)
		b = append(b, ',')
	}
	if len(ss) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendIntSlice(b []byte, ints []int, quote bool) []byte {
	if ints == nil {
		return appendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	b = append(b, '{')
	for _, n := range ints {
		b = strconv.AppendInt(b, int64(n), 10)
		b = append(b, ',')
	}
	if len(ints) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendInt64Slice(b []byte, ints []int64, quote bool) []byte {
	if ints == nil {
		return appendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	b = append(b, "{"...)
	for _, n := range ints {
		b = strconv.AppendInt(b, n, 10)
		b = append(b, ',')
	}
	if len(ints) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendFloat64Slice(b []byte, floats []float64, quote bool) []byte {
	if floats == nil {
		return appendNull(b, quote)
	}

	if quote {
		b = append(b, '\'')
	}

	b = append(b, "{"...)
	for _, n := range floats {
		b = appendFloat(b, n)
		b = append(b, ',')
	}
	if len(floats) > 0 {
		b[len(b)-1] = '}' // Replace trailing comma.
	} else {
		b = append(b, '}')
	}

	if quote {
		b = append(b, '\'')
	}

	return b
}

func appendDriverValuer(b []byte, v driver.Valuer, quote bool) []byte {
	value, err := v.Value()
	if err != nil {
		log.Printf("%T value failed: %s", v, err)
		return appendNull(b, quote)
	}
	return appendIface(b, value, quote)
}

func appendField(b []byte, f string) []byte {
	b = append(b, '"')
	for _, c := range []byte(f) {
		if c == '"' {
			b = append(b, '"', '"')
		} else {
			b = append(b, c)
		}
	}
	b = append(b, '"')
	return b
}

func appendTime(b []byte, tm time.Time, quote bool) []byte {
	if quote {
		b = append(b, '\'')
	}
	b = pgutil.AppendTime(b, tm)
	if quote {
		b = append(b, '\'')
	}
	return b
}

//------------------------------------------------------------------------------

func formatQuery(dst, src []byte, params []interface{}) ([]byte, error) {
	if len(params) == 0 {
		return append(dst, src...), nil
	}

	var model *Model
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
			// Lazily initialize Model.
			if model == nil {
				if len(params) == 0 {
					return nil, errorf("pg: expected at least one parameter, got nothing")
				}
				last := params[len(params)-1]
				params = params[:len(params)-1]
				if v, ok := last.(*Model); ok {
					model = v
				} else {
					model = NewModel(last, "")
				}
			}
			var err error
			dst, err = model.appendName(dst, name)
			if err != nil {
				return nil, err
			}
		} else {
			if paramInd >= len(params) {
				return nil, errorf(
					"pg: expected at least %d parameters, got %d",
					paramInd+1, len(params),
				)
			}

			dst = appendIface(dst, params[paramInd], true)
			paramInd++
		}
	}

	if paramInd < len(params) {
		return nil, errorf("pg: expected %d parameters, got %d", paramInd, len(params))
	}

	return dst, nil
}
