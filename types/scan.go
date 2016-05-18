package types

import (
	"database/sql"
	"encoding/hex"
	"reflect"
	"strconv"
	"time"

	"gopkg.in/pg.v4/internal"
)

func Scan(v interface{}, b []byte) error {
	switch v := v.(type) {
	case *string:
		*v = string(b)
		return nil
	case *[]byte:
		if b == nil {
			*v = nil
			return nil
		}
		var err error
		*v, err = scanBytes(b)
		return err
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
		*v, err = ParseTime(b)
		return err
	}

	vv := reflect.ValueOf(v)
	if !vv.IsValid() {
		return internal.Errorf("pg: Scan(nil)")
	}
	if vv.Kind() != reflect.Ptr {
		return internal.Errorf("pg: Scan(nonsettable %T)", v)
	}
	vv = vv.Elem()
	if !vv.IsValid() {
		return internal.Errorf("pg: Scan(nonsettable %T)", v)
	}
	return ScanValue(vv, b)
}

func scanSQLScanner(scanner sql.Scanner, b []byte) error {
	if b == nil {
		return scanner.Scan(nil)
	}
	return scanner.Scan(b)
}

func scanBytes(b []byte) ([]byte, error) {
	if len(b) < 2 {
		return nil, internal.Errorf("pg: can't parse bytes: %q", b)
	}

	b = b[2:] // Trim off "\\x".
	tmp := make([]byte, hex.DecodedLen(len(b)))
	_, err := hex.Decode(tmp, b)
	return tmp, err
}

func scanStringStringMap(f []byte) (map[string]string, error) {
	p := newHstoreParser(f)
	m := make(map[string]string)
	for p.Valid() {
		key, err := p.NextKey()
		if err != nil {
			return nil, err
		}
		if key == nil {
			return nil, internal.Errorf("pg: unexpected NULL: %q", f)
		}
		value, err := p.NextValue()
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, internal.Errorf("pg: unexpected NULL: %q", f)
		}
		m[string(key)] = string(value)
	}
	return m, nil
}
