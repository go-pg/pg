package types

import (
	"database/sql/driver"
	"encoding/hex"
	"reflect"
	"strconv"
	"time"

	"gopkg.in/pg.v4/internal/parser"
)

func Append(b []byte, v interface{}, quote bool) []byte {
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
		return appendFloat(b, float64(v))
	case float64:
		return appendFloat(b, v)
	case string:
		return AppendString(b, v, quote)
	case time.Time:
		return AppendTime(b, v, quote)
	case []byte:
		return appendBytes(b, v, quote)
	case ValueAppender:
		b, err := v.AppendValue(b, quote)
		if err != nil {
			panic(err)
		}
		return b
	case driver.Valuer:
		return appendDriverValuer(b, v, quote)
	default:
		return appendValue(b, reflect.ValueOf(v), quote)
	}
}

func AppendNull(b []byte, quote bool) []byte {
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

func AppendString(b []byte, s string, quote bool) []byte {
	return AppendStringBytes(b, []byte(s), quote)
}

func AppendStringBytes(b []byte, bytes []byte, quote bool) []byte {
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
		return AppendNull(b, quote)
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

func AppendStringStringMap(b []byte, m map[string]string, quote bool) []byte {
	if m == nil {
		return AppendNull(b, quote)
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

func appendDriverValuer(b []byte, v driver.Valuer, quote bool) []byte {
	value, err := v.Value()
	if err != nil {
		panic(err)
	}
	return Append(b, value, quote)
}

func AppendField(b []byte, field string, quote bool) []byte {
	return AppendFieldBytes(b, []byte(field), quote)
}

func AppendFieldBytes(b []byte, field []byte, quote bool) []byte {
	p := parser.New(field)
	var quoted bool
	for p.Valid() {
		c := p.Read()

		switch c {
		case '\\':
			if p.Got("?") {
				c = '?'
			}
		case '*':
			if !quoted {
				b = append(b, '*')
				continue
			}
		case '.':
			if quote {
				b = append(b, '"')
			}
			b = append(b, '.')
			if p.Got("*") {
				b = append(b, '*')
				quoted = false
			} else if quote {
				b = append(b, '"')
			}
			continue
		case ' ':
			if p.Got("AS ") || p.Got("as ") {
				if quote {
					b = append(b, '"')
				}
				b = append(b, ` AS `...)
				if quote {
					b = append(b, '"')
				}
			} else {
				b = append(b, ' ')
			}
			continue
		case '?':
			b = append(b, '?')
			b = append(b, p.ReadIdentifier()...)
			continue
		}

		if quote && !quoted {
			b = append(b, '"')
			quoted = true
		}
		if quote && c == '"' {
			b = append(b, '"', '"')
		} else {
			b = append(b, c)
		}

	}
	if quote && quoted {
		b = append(b, '"')
	}
	return b
}
