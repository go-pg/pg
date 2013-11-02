package pg

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

func AppendQ(dst []byte, src string, args ...interface{}) ([]byte, error) {
	p := newQueryFormatter(dst, src)
	for _, arg := range args {
		if err := p.Format(arg); err != nil {
			return nil, err
		}
	}
	return p.Value()
}

func FormatQ(src string, args ...interface{}) (Q, error) {
	b, err := AppendQ(nil, src, args...)
	if err != nil {
		return "", err
	}
	return Q(b), nil
}

func MustFormatQ(src string, args ...interface{}) Q {
	q, err := FormatQ(src, args...)
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

func appendValue(dst []byte, srci interface{}) []byte {
	switch src := srci.(type) {
	case bool:
		if src {
			return append(dst, "'t'"...)
		}
		return append(dst, "'f'"...)
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
	case string:
		return appendString(dst, src)
	case time.Time:
		dst = append(dst, '\'')
		dst = append(dst, src.UTC().Format(datetimeFormat)...)
		dst = append(dst, '\'')
		return dst
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
	default:
		panic(fmt.Sprintf("pg: unsupported src type: %T", srci))
	}
}

func appendRawValue(dst []byte, srci interface{}) []byte {
	switch src := srci.(type) {
	case bool:
		if src {
			return append(dst, 't')
		}
		return append(dst, 'f')
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
	case string:
		return appendRawString(dst, src)
	case time.Time:
		return append(dst, src.UTC().Format(datetimeFormat)...)
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
	default:
		panic(fmt.Sprintf("pg: unsupported src type: %T", srci))
	}
}

//------------------------------------------------------------------------------

type queryFormatter struct {
	*parser
	dst []byte
}

func newQueryFormatter(dst []byte, src string) *queryFormatter {
	return &queryFormatter{
		parser: &parser{b: []byte(src)},
		dst:    dst,
	}
}

func (f *queryFormatter) Format(v interface{}) (err error) {
	for f.Valid() {
		c := f.Next()
		if c == '?' {
			f.dst = appendValue(f.dst, v)
			return nil
		}
		f.dst = append(f.dst, c)
	}
	if err != nil {
		return err
	}
	return errExpectedPlaceholder
}

func (f *queryFormatter) Value() ([]byte, error) {
	for f.Valid() {
		c := f.Next()
		if c == '?' {
			return nil, errUnexpectedPlaceholder
		}
		f.dst = append(f.dst, c)
	}
	return f.dst, nil
}
