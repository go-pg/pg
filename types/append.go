package types

import (
	"database/sql/driver"
	"encoding/hex"
	"math"
	"reflect"
	"strconv"
	"time"
)

func Append(b []byte, v interface{}, flags int) []byte {
	switch v := v.(type) {
	case nil:
		return AppendNull(b, flags)
	case bool:
		return appendBool(b, v)
	case int32:
		return strconv.AppendInt(b, int64(v), 10)
	case int64:
		return strconv.AppendInt(b, v, 10)
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case float32:
		return appendFloat(b, float64(v), flags, 32)
	case float64:
		return appendFloat(b, v, flags, 64)
	case string:
		return AppendString(b, v, flags)
	case time.Time:
		return AppendTime(b, v, flags)
	case []byte:
		return AppendBytes(b, v, flags)
	case ValueAppender:
		return appendAppender(b, v, flags)
	case driver.Valuer:
		return appendDriverValuer(b, v, flags)
	default:
		return appendValue(b, reflect.ValueOf(v), flags)
	}
}

func AppendError(b []byte, err error) []byte {
	b = append(b, "?!("...)
	b = append(b, err.Error()...)
	b = append(b, ')')
	return b
}

func AppendNull(b []byte, flags int) []byte {
	if hasFlag(flags, quoteFlag) {
		return append(b, "NULL"...)
	}
	return nil
}

func appendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, "TRUE"...)
	}
	return append(dst, "FALSE"...)
}

func appendFloat(dst []byte, v float64, flags int, bitSize int) []byte {
	if hasFlag(flags, arrayFlag) {
		return appendFloat2(dst, v, flags)
	}

	switch {
	case math.IsNaN(v):
		if hasFlag(flags, quoteFlag) {
			return append(dst, "'NaN'"...)
		}
		return append(dst, "NaN"...)
	case math.IsInf(v, 1):
		if hasFlag(flags, quoteFlag) {
			return append(dst, "'Infinity'"...)
		}
		return append(dst, "Infinity"...)
	case math.IsInf(v, -1):
		if hasFlag(flags, quoteFlag) {
			return append(dst, "'-Infinity'"...)
		}
		return append(dst, "-Infinity"...)
	default:
		return strconv.AppendFloat(dst, v, 'f', -1, bitSize)
	}
}

func appendFloat2(dst []byte, v float64, _ int) []byte {
	switch {
	case math.IsNaN(v):
		return append(dst, "NaN"...)
	case math.IsInf(v, 1):
		return append(dst, "Infinity"...)
	case math.IsInf(v, -1):
		return append(dst, "-Infinity"...)
	default:
		return strconv.AppendFloat(dst, v, 'f', -1, 64)
	}
}

func AppendString(b []byte, s string, flags int) []byte {
	if hasFlag(flags, arrayFlag) {
		return appendString2(b, s, flags)
	}

	if hasFlag(flags, quoteFlag) {
		b = append(b, '\'')
		for i := 0; i < len(s); i++ {
			c := s[i]

			if c == '\000' {
				continue
			}

			if c == '\'' {
				b = append(b, '\'', '\'')
			} else {
				b = append(b, c)
			}
		}
		b = append(b, '\'')
		return b
	}

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '\000' {
			b = append(b, c)
		}
	}
	return b
}

func appendString2(b []byte, s string, flags int) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]

		if c == '\000' {
			continue
		}

		switch c {
		case '\'':
			if hasFlag(flags, quoteFlag) {
				b = append(b, '\'')
			}
			b = append(b, '\'')
		case '"':
			b = append(b, '\\', '"')
		case '\\':
			b = append(b, '\\', '\\')
		default:
			b = append(b, c)
		}
	}
	b = append(b, '"')
	return b
}

func AppendBytes(b []byte, bytes []byte, flags int) []byte {
	if bytes == nil {
		return AppendNull(b, flags)
	}

	if hasFlag(flags, arrayFlag) {
		b = append(b, '"')
	} else if hasFlag(flags, quoteFlag) {
		b = append(b, '\'')
	}

	tmp := make([]byte, hex.EncodedLen(len(bytes)))
	hex.Encode(tmp, bytes)

	if hasFlag(flags, arrayFlag) {
		b = append(b, '\\')
	}
	b = append(b, "\\x"...)
	b = append(b, tmp...)

	if hasFlag(flags, arrayFlag) {
		b = append(b, '"')
	} else if hasFlag(flags, quoteFlag) {
		b = append(b, '\'')
	}

	return b
}

func appendDriverValuer(b []byte, v driver.Valuer, flags int) []byte {
	value, err := v.Value()
	if err != nil {
		return AppendError(b, err)
	}
	return Append(b, value, flags)
}

func appendAppender(b []byte, v ValueAppender, flags int) []byte {
	bb, err := v.AppendValue(b, flags)
	if err != nil {
		return AppendError(b, err)
	}
	return bb
}
