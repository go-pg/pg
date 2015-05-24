package pg

import (
	"database/sql"
	"encoding/hex"
	"reflect"
	"strconv"
)

func Decode(dst interface{}, b []byte) error {
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
