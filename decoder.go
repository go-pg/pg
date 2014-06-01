package pg

import (
	"database/sql"
	"encoding/hex"
	"reflect"
	"strconv"
	"time"

	"github.com/golang/glog"
)

var (
	timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
)

func Decode(dst interface{}, f []byte) error {
	if tm, ok := dst.(*time.Time); ok {
		tm_, err := decodeTime(f)
		if err != nil {
			return err
		}
		*tm = tm_
		return nil
	}

	if unmarshaler, ok := dst.(textUnmarshaler); ok {
		return unmarshaler.UnmarshalText(f)
	}

	if scanner, ok := dst.(sql.Scanner); ok {
		glog.Infof("scan %v %q", scanner, f)
		if f == nil {
			return scanner.Scan(nil)
		}
		return scanner.Scan(f)
	}

	// NULL.
	if f == nil {
		return nil
	}

	switch v := dst.(type) {
	case *bool:
		if len(f) == 1 && f[0] == 't' {
			*v = true
		} else {
			*v = false
		}
		return nil
	case *int8:
		n, err := strconv.ParseInt(string(f), 10, 8)
		if err != nil {
			return err
		}
		*v = int8(n)
		return nil
	case *int16:
		n, err := strconv.ParseInt(string(f), 10, 16)
		if err != nil {
			return err
		}
		*v = int16(n)
		return nil
	case *int32:
		n, err := strconv.ParseInt(string(f), 10, 32)
		if err != nil {
			return err
		}
		*v = int32(n)
		return nil
	case *int64:
		n, err := strconv.ParseInt(string(f), 10, 64)
		if err != nil {
			return err
		}
		*v = n
		return nil
	case *int:
		n, err := strconv.ParseInt(string(f), 10, 64)
		if err != nil {
			return err
		}
		*v = int(n)
		return nil
	case *uint8:
		n, err := strconv.ParseUint(string(f), 10, 8)
		if err != nil {
			return err
		}
		*v = uint8(n)
		return nil
	case *uint16:
		n, err := strconv.ParseUint(string(f), 10, 16)
		if err != nil {
			return err
		}
		*v = uint16(n)
		return nil
	case *uint32:
		n, err := strconv.ParseUint(string(f), 10, 32)
		if err != nil {
			return err
		}
		*v = uint32(n)
		return nil
	case *uint64:
		n, err := strconv.ParseUint(string(f), 10, 64)
		if err != nil {
			return err
		}
		*v = n
		return nil
	case *uint:
		n, err := strconv.ParseInt(string(f), 10, 64)
		if err != nil {
			return err
		}
		*v = uint(n)
		return nil
	case *float32:
		n, err := strconv.ParseFloat(string(f), 32)
		if err != nil {
			return err
		}
		*v = float32(n)
		return nil
	case *float64:
		n, err := strconv.ParseFloat(string(f), 64)
		if err != nil {
			return err
		}
		*v = n
		return nil
	case *string:
		*v = string(f)
		return nil
	case *[]byte:
		b, err := decodeBytes(f)
		if err != nil {
			return err
		}
		*v = b
		return nil
	case *[]string:
		s, err := decodeStringSlice(f)
		if err != nil {
			return err
		}
		*v = s
		return nil
	case *[]int:
		s, err := decodeIntSlice(f)
		if err != nil {
			return err
		}
		*v = s
		return nil
	case *[]int64:
		s, err := decodeInt64Slice(f)
		if err != nil {
			return err
		}
		*v = s
		return nil
	case *map[string]string:
		m, err := decodeStringStringMap(f)
		if err != nil {
			return err
		}
		*v = m
		return nil
	}

	v := reflect.ValueOf(dst)
	if !v.IsValid() {
		return errorf("pg: Decode(%s)", v)
	}
	return DecodeValue(v.Elem(), f)
}

func DecodeValue(dst reflect.Value, f []byte) error {
	// NULL.
	if f == nil {
		return nil
	}

	kind := dst.Kind()
	if kind == reflect.Struct && dst.Type() == timeType {
		tm, err := decodeTime(f)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(tm))
		return nil
	}

	if dst.CanAddr() {
		addr := dst.Addr()
		if addr.CanInterface() {
			if unmarshaler, ok := addr.Interface().(textUnmarshaler); ok {
				return unmarshaler.UnmarshalText(f)
			}
		}
	}

	switch dst.Kind() {
	case reflect.Ptr:
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return DecodeValue(dst.Elem(), f)
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
		n, err := strconv.ParseInt(string(f), 10, 64)
		if err != nil {
			return err
		}
		dst.SetUint(uint64(n))
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
	case reflect.Interface:
		return DecodeValue(dst.Elem(), f)
	}
	return errorf("pg: unsupported dst: %s", dst)
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
	return errorf("pg: unsupported dst: %s", dst)
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
	return errorf("pg: unsupported dst: %s", dst)
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
		if c := f[len(f)-5]; c == '+' || c == '-' {
			return time.Parse(timestampWithTzFormat, string(f))
		}
		if c := f[len(f)-3]; c == '+' || c == '-' {
			return time.Parse(timestampWithTzFormat2, string(f))
		}
		return time.Parse(timestampFormat, string(f))
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
