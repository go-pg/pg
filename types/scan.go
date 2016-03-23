package types

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

func Scan(dst interface{}, b []byte) error {
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
		*v, err = ParseTime(b)
		return err
	}

	v := reflect.ValueOf(dst)
	if !v.IsValid() {
		return fmt.Errorf("pg: Scan(nil)")
	}
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("pg: Scan(nonsettable %T)", dst)
	}
	vv := v.Elem()
	if !vv.IsValid() {
		return fmt.Errorf("pg: Scan(nonsettable %T)", dst)
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
		return nil, fmt.Errorf("pg: can't parse bytes: %q", b)
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
			return nil, fmt.Errorf("pg: unexpected NULL: %q", f)
		}
		value, err := p.NextValue()
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, fmt.Errorf("pg: unexpected NULL: %q", f)
		}
		m[string(key)] = string(value)
	}
	return m, nil
}
