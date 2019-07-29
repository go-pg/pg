package types

import (
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/go-pg/pg/v9/internal"
)

func Scan(v interface{}, rd Reader, n int) error {
	var err error
	switch v := v.(type) {
	case *string:
		*v, err = ScanString(rd, n)
		return err
	case *[]byte:
		*v, err = ScanBytes(rd, n)
		return err
	case *int:
		*v, err = ScanInt(rd, n)
		return err
	case *int64:
		*v, err = ScanInt64(rd, n)
		return err
	case *float32:
		*v, err = ScanFloat32(rd, n)
		return err
	case *float64:
		*v, err = ScanFloat64(rd, n)
		return err
	case *time.Time:
		*v, err = ScanTime(rd, n)
		return err
	}

	vv := reflect.ValueOf(v)
	if !vv.IsValid() {
		return errors.New("pg: Scan(nil)")
	}
	if vv.Kind() != reflect.Ptr {
		return fmt.Errorf("pg: Scan(nonsettable %T)", v)
	}
	vv = vv.Elem()
	if !vv.IsValid() {
		return fmt.Errorf("pg: Scan(nonsettable %T)", v)
	}
	return ScanValue(vv, rd, n)
}

func ScanString(rd Reader, n int) (string, error) {
	if n <= 0 {
		return "", nil
	}

	b, err := rd.ReadFull()
	if err != nil {
		return "", err
	}

	return internal.BytesToString(b), nil
}

func ScanBytes(rd Reader, n int) ([]byte, error) {
	if n == -1 {
		return nil, nil
	}
	if n == 0 {
		return []byte{}, nil
	}
	return readBytes(rd, nil)
}

func readBytes(rd Reader, b []byte) ([]byte, error) {
	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return nil, err
	}

	if len(tmp) < 2 {
		return nil, fmt.Errorf("pg: can't parse bytea: %q", tmp)
	}

	if tmp[0] != '\\' || tmp[1] != 'x' {
		return nil, fmt.Errorf("pg: can't parse bytea: %q", tmp)
	}
	tmp = tmp[2:] // Trim off "\\x".

	if b == nil {
		b = make([]byte, hex.DecodedLen(len(tmp)))
	}
	written, err := hex.Decode(b, tmp)
	if err != nil {
		return nil, err
	}

	return b[:written], err
}

func ScanInt(rd Reader, n int) (int, error) {
	if n <= 0 {
		return 0, nil
	}

	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return 0, err
	}

	num, err := internal.Atoi(tmp)
	if err != nil {
		return 0, err
	}

	return num, nil
}

func ScanInt64(rd Reader, n int) (int64, error) {
	if n <= 0 {
		return 0, nil
	}

	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return 0, err
	}

	num, err := internal.ParseInt(tmp, 10, 64)
	if err != nil {
		return 0, err
	}

	return num, nil
}

func ScanUint64(rd Reader, n int) (uint64, error) {
	if n <= 0 {
		return 0, nil
	}

	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return 0, err
	}

	// PostgreSQL does not natively support uint64 - only int64.
	// Be nice and accept negative int64.
	if len(tmp) > 0 && tmp[0] == '-' {
		num, err := internal.ParseInt(tmp, 10, 64)
		if err != nil {
			return 0, err
		}
		return uint64(num), nil
	}

	num, err := internal.ParseUint(tmp, 10, 64)
	if err != nil {
		return 0, err
	}

	return num, nil
}

func ScanFloat32(rd Reader, n int) (float32, error) {
	if n <= 0 {
		return 0, nil
	}

	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return 0, err
	}

	num, err := internal.ParseFloat(tmp, 32)
	if err != nil {
		return 0, err
	}

	return float32(num), nil
}

func ScanFloat64(rd Reader, n int) (float64, error) {
	if n <= 0 {
		return 0, nil
	}

	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return 0, err
	}

	num, err := internal.ParseFloat(tmp, 64)
	if err != nil {
		return 0, err
	}

	return num, nil
}

func ScanTime(rd Reader, n int) (time.Time, error) {
	if n <= 0 {
		return time.Time{}, nil
	}

	tmp, err := rd.ReadFullTemp()
	if err != nil {
		return time.Time{}, err
	}

	return ParseTime(tmp)
}
