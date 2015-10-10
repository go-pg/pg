package pg

import (
	"database/sql"
	"encoding/hex"
	"reflect"
	"strconv"
	"time"

	"gopkg.in/pg.v3/pgutil"
)

func Decode(dst interface{}, b []byte) error {
	switch v := dst.(type) {
	case *string:
		*v = string(b)
		return nil
	case *int:
		if b == nil {
			*v = 0
			return nil
		}
		var err error
		*v, err = strconv.Atoi(string(b))
		return err
	case *int32:
		if b == nil {
			*v = 0
			return nil
		}
		n, err := strconv.ParseInt(string(b), 10, 32)
		*v = int32(n)
		return err
	case *int64:
		if b == nil {
			*v = 0
			return nil
		}
		var err error
		*v, err = strconv.ParseInt(string(b), 10, 64)
		return err
	case *time.Time:
		if b == nil {
			*v = time.Time{}
			return nil
		}
		var err error
		*v, err = pgutil.ParseTime(b)
		return err
	}

	v := reflect.ValueOf(dst)
	if !v.IsValid() {
		return errorf("pg: Decode(nil)")
	}
	if v.Kind() != reflect.Ptr {
		return errorf("pg: Decode(nonsettable %T)", dst)
	}
	vv := v.Elem()
	if !vv.IsValid() {
		return errorf("pg: Decode(nonsettable %T)", dst)
	}
	return DecodeValue(vv, b)
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

func decodeFloat64Slice(b []byte) ([]float64, error) {
	p := newArrayParser(b[1 : len(b)-1])
	slice := make([]float64, 0)
	for p.Valid() {
		elem, err := p.NextElem()
		if err != nil {
			return nil, err
		}
		if elem == nil {
			return nil, errorf("pg: unexpected NULL: %q", b)
		}
		n, err := strconv.ParseFloat(string(elem), 64)
		if err != nil {
			return nil, err
		}
		slice = append(slice, n)
	}
	return slice, nil
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
