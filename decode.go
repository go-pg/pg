package pg

import (
	"database/sql"
	"encoding/hex"
	"reflect"
	"strconv"
	"time"
)

func Decode(dst interface{}, f []byte) error {
	if err, ok := tryDecodeInterfaces(dst, f); ok {
		return err
	}

	v := reflect.ValueOf(dst)
	if !v.IsValid() || v.Kind() != reflect.Ptr {
		return decodeError(v)
	}
	vv := v.Elem()
	if !vv.IsValid() {
		return decodeError(v)
	}
	return DecodeValue(vv, f)
}

func decodeError(v reflect.Value) error {
	if !v.IsValid() {
		return errorf("pg: Decode(nil)")
	}
	if !v.CanSet() {
		return errorf("pg: Decode(nonsettable %s)", v.Type())
	}
	if v.Kind() == reflect.Interface {
		return errorf("pg: Decode(nil)")
	}
	return errorf("pg: Decode(nil %s)", v.Type())
}

func tryDecodeInterfaces(dst interface{}, b []byte) (error, bool) {
	if scanner, ok := dst.(sql.Scanner); ok {
		return decodeScanner(scanner, b), true
	}
	return nil, false
}

func decodeScanner(scanner sql.Scanner, b []byte) error {
	if b == nil {
		return scanner.Scan(nil)
	}
	return scanner.Scan(b)
}

func decodeBytes(f []byte) ([]byte, error) {
	f = f[2:] // Trim off "\\x".
	b := make([]byte, hex.DecodedLen(len(f)))
	_, err := hex.Decode(b, f)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func decodeTime(f []byte) (time.Time, error) {
	switch l := len(f); {
	case l <= len(dateFormat):
		return time.Parse(dateFormat, string(f))
	case l <= len(timeFormat):
		return time.Parse(timeFormat, string(f))
	default:
		if c := f[len(f)-6]; c == '+' || c == '-' {
			return time.Parse(timestamptzFormat, string(f))
		}
		if c := f[len(f)-3]; c == '+' || c == '-' {
			return time.Parse(timestamptzFormat2, string(f))
		}
		if c := f[len(f)-9]; c == '+' || c == '-' {
			return time.Parse(timestamptzFormat3, string(f))
		}
		return time.ParseInLocation(timestampFormat, string(f), time.Local)
	}
}

func decodeIntSlice(f []byte) ([]int, error) {
	p := newArrayParser(f[1 : len(f)-1])
	s := make([]int, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, errorf("pg: unexpected NULL: %q", f)
		}
		n, err := strconv.Atoi(string(elem))
		if err != nil {
			return nil, err
		}
		s = append(s, n)
	}
	return s, nil
}

func decodeInt64Slice(f []byte) ([]int64, error) {
	p := newArrayParser(f[1 : len(f)-1])
	s := make([]int64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, errorf("pg: unexpected NULL: %q", f)
		}
		n, err := strconv.ParseInt(string(elem), 10, 64)
		if err != nil {
			return nil, err
		}
		s = append(s, n)
	}
	return s, nil
}

func decodeStringSlice(f []byte) ([]string, error) {
	p := newArrayParser(f[1 : len(f)-1])
	s := make([]string, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, errorf("pg: unexpected NULL: %q", f)
		}
		s = append(s, string(elem))
	}
	return s, nil
}

func decodeStringStringMap(f []byte) (map[string]string, error) {
	p := newHstoreParser(f)
	m := make(map[string]string)
	for p.Valid() {
		key, err := p.NextKey()
		if err != nil {
			return nil, err
		}
		if key == nil {
			return nil, errorf("pg: unexpected NULL: %q", f)
		}
		value, err := p.NextValue()
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, errorf("pg: unexpected NULL: %q", f)
		}
		m[string(key)] = string(value)
	}
	return m, nil
}
