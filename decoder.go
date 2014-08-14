package pg

import (
	"database/sql"
	"encoding/hex"
	"reflect"
	"strconv"
	"time"
)

var (
	timePtrType = reflect.TypeOf((*time.Time)(nil))
	timeType    = timePtrType.Elem()
)

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

func decodeNull(dst reflect.Value) error {
	kind := dst.Kind()
	if kind == reflect.Interface {
		return decodeNull(dst.Elem())
	}
	if dst.CanSet() {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}
	if kind == reflect.Ptr {
		return decodeNull(dst.Elem())
	}
	return nil
}

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

func DecodeValue(dst reflect.Value, f []byte) error {
	if !dst.IsValid() {
		return decodeError(dst)
	}

	if f == nil {
		return decodeNull(dst)
	}

	kind := dst.Kind()
	if kind == reflect.Ptr && dst.IsNil() && dst.CanSet() {
		dst.Set(reflect.New(dst.Type().Elem()))
	}

	if err, ok := tryDecodeInterfaces(dst.Interface(), f); ok {
		return err
	}

	if kind == reflect.Interface || kind == reflect.Ptr {
		v := dst.Elem()
		if !v.IsValid() {
			return decodeError(dst)
		}
		return DecodeValue(v, f)
	}

	if !dst.CanSet() {
		return decodeError(dst)
	}

	switch kind {
	case reflect.Bool:
		if len(f) == 1 && f[0] == 't' {
			dst.SetBool(true)
		} else {
			dst.SetBool(false)
		}
		return nil
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		n, err := strconv.ParseInt(string(f), 10, 64)
		if err != nil {
			return err
		}
		dst.SetInt(n)
		return nil
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		n, err := strconv.ParseUint(string(f), 10, 64)
		if err != nil {
			return err
		}
		dst.SetUint(n)
		return nil
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(string(f), 64)
		if err != nil {
			return err
		}
		dst.SetFloat(n)
		return nil
	case reflect.String:
		dst.SetString(string(f))
		return nil
	case reflect.Slice:
		return decodeSliceValue(dst, f)
	case reflect.Map:
		return decodeMapValue(dst, f)
	case reflect.Struct:
		if dst.Type() == timeType {
			tm, err := decodeTime(f)
			if err != nil {
				return err
			}
			dst.Set(reflect.ValueOf(tm))
			return nil
		}
	}
	return errorf("pg: unsupported dst: %s", dst.Type())
}

func tryDecodeInterfaces(dst interface{}, f []byte) (error, bool) {
	if scanner, ok := dst.(sql.Scanner); ok {
		if f == nil {
			return scanner.Scan(nil), true
		}
		return scanner.Scan(f), true
	}
	return nil, false
}

func decodeSliceValue(dst reflect.Value, f []byte) error {
	elemType := dst.Type().Elem()
	switch elemType.Kind() {
	case reflect.Uint8:
		b, err := decodeBytes(f)
		if err != nil {
			return err
		}
		dst.SetBytes(b)
		return nil
	case reflect.String:
		s, err := decodeStringSlice(f)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
		return nil
	case reflect.Int:
		s, err := decodeIntSlice(f)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
		return nil
	case reflect.Int64:
		s, err := decodeInt64Slice(f)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(s))
		return nil
	}
	return errorf("pg: unsupported dst: %s", dst.Type())
}

func decodeMapValue(dst reflect.Value, f []byte) error {
	typ := dst.Type()
	if typ.Key().Kind() == reflect.String && typ.Elem().Kind() == reflect.String {
		m, err := decodeStringStringMap(f)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(m))
		return nil
	}
	return errorf("pg: unsupported dst: %s", dst.Type())
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
