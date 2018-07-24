package types

import (
	"database/sql/driver"
	"encoding/hex"
	"math"
	"reflect"
	"strconv"
	"time"
)

func Append(b []byte, v interface{}, quote int) []byte {
	switch v := v.(type) {
	case nil:
		return AppendNull(b, quote)
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
		return appendFloat(b, float64(v), quote)
	case float64:
		return appendFloat(b, v, quote)
	case string:
		return AppendString(b, v, quote)
	case time.Time:
		return AppendTime(b, v, quote)
	case []byte:
		return AppendBytes(b, v, quote)
	case ValueAppender:
		return appendAppender(b, v, quote)
	case driver.Valuer:
		return appendDriverValuer(b, v, quote)
	default:
		return appendValue(b, reflect.ValueOf(v), quote)
	}
}

func AppendError(b []byte, err error) []byte {
	b = append(b, "?!("...)
	b = append(b, err.Error()...)
	b = append(b, ')')
	return b
}

func AppendNull(b []byte, quote int) []byte {
	if quote == 1 {
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

func appendFloat(dst []byte, v float64, quote int) []byte {
	switch {
	case math.IsNaN(v):
		if quote == 1 {
			return append(dst, "'NaN'"...)
		}
		return append(dst, "NaN"...)
	case math.IsInf(v, 1):
		if quote == 1 {
			return append(dst, "'Infinity'"...)
		}
		return append(dst, "Infinity"...)
	case math.IsInf(v, -1):
		if quote == 1 {
			return append(dst, "'-Infinity'"...)
		}
		return append(dst, "-Infinity"...)
	default:
		return strconv.AppendFloat(dst, v, 'f', -1, 64)
	}
}

func AppendString(b []byte, s string, quote int) []byte {
	if quote == 2 {
		b = append(b, '"')
	} else if quote == 1 {
		b = append(b, '\'')
	}

	for i := 0; i < len(s); i++ {
		c := s[i]

		if c == '\000' {
			continue
		}

		if quote >= 1 {
			if c == '\'' {
				b = append(b, '\'', '\'')
				continue
			}
		}

		if quote == 2 {
			if c == '"' {
				b = append(b, '\\', '"')
				continue
			}
			if c == '\\' {
				b = append(b, '\\', '\\')
				continue
			}
		}

		b = append(b, c)
	}

	if quote >= 2 {
		b = append(b, '"')
	} else if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func AppendBytes(b []byte, bytes []byte, quote int) []byte {
	if bytes == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	tmp := make([]byte, hex.EncodedLen(len(bytes)))
	hex.Encode(tmp, bytes)
	b = append(b, "\\x"...)
	b = append(b, tmp...)

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func AppendStringStringMap(b []byte, m map[string]string, quote int) []byte {
	if m == nil {
		return AppendNull(b, quote)
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	for key, value := range m {
		b = AppendString(b, key, 2)
		b = append(b, '=', '>')
		b = AppendString(b, value, 2)
		b = append(b, ',')
	}
	if len(m) > 0 {
		b = b[:len(b)-1] // Strip trailing comma.
	}

	if quote == 1 {
		b = append(b, '\'')
	}

	return b
}

func appendDriverValuer(b []byte, v driver.Valuer, quote int) []byte {
	value, err := v.Value()
	if err != nil {
		return AppendError(b, err)
	}
	return Append(b, value, quote)
}

func appendAppender(b []byte, v ValueAppender, quote int) []byte {
	return v.AppendValue(b, quote)
}
