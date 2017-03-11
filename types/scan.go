package types

import (
	"database/sql"
	"encoding/hex"
	"reflect"
	"strconv"
	"time"

	"github.com/go-pg/pg/internal"
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
		*v, err = strconv.Atoi(internal.BytesToString(b))
		return err
	case *int64:
		if b == nil {
			*v = 0
			return nil
		}
		var err error
		*v, err = strconv.ParseInt(internal.BytesToString(b), 10, 64)
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
		return internal.Errorf("pg: Scan(non-pointer %T)", v)
	}
	vv = vv.Elem()
	if !vv.IsValid() {
		return internal.Errorf("pg: Scan(non-pointer %T)", v)
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
