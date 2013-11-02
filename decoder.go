package pg

import (
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

const (
	dateFormat     = "2006-01-02"
	timeFormat     = "15:04:05.999999999"
	datetimeFormat = "2006-01-02 15:04:05.999999999"
)

func Decode(dst interface{}, f []byte) error {
	// NULL.
	if f == nil {
		return nil
	}

	switch v := dst.(type) {
	case *bool:
		if len(f) == 1 && f[0] == 't' {
			*v = true
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
		*v = int64(n)
		return nil
	case *int:
		n, err := strconv.ParseInt(string(f), 10, 64)
		if err != nil {
			return err
		}
		*v = int(n)
		return nil
	case *uint8:
		n, err := strconv.ParseInt(string(f), 10, 8)
		if err != nil {
			return err
		}
		*v = uint8(n)
		return nil
	case *uint16:
		n, err := strconv.ParseInt(string(f), 10, 16)
		if err != nil {
			return err
		}
		*v = uint16(n)
		return nil
	case *uint32:
		n, err := strconv.ParseInt(string(f), 10, 32)
		if err != nil {
			return err
		}
		*v = uint32(n)
		return nil
	case *uint64:
		n, err := strconv.ParseInt(string(f), 10, 64)
		if err != nil {
			return err
		}
		*v = uint64(n)
		return nil
	case *uint:
		n, err := strconv.ParseInt(string(f), 10, 64)
		if err != nil {
			return err
		}
		*v = uint(n)
		return nil
	case *string:
		*v = string(f)
		return nil
	case *[]byte:
		f = f[2:] // Trim off "\\x".
		d := make([]byte, hex.DecodedLen(len(f)))
		_, err := hex.Decode(d, f)
		if err != nil {
			return err
		}
		*v = d
		return nil
	case *time.Time:
		var format string
		switch l := len(f); {
		case l <= len(dateFormat):
			format = dateFormat
		case l <= len(timeFormat):
			format = timeFormat
		default:
			format = datetimeFormat
		}

		tm, err := time.Parse(format, string(f))
		if err != nil {
			return err
		}
		*v = tm.UTC()

		return nil
	case *[]string:
		p := newArrayParser(f[1 : len(f)-1])
		vv := make([]string, 0)
		for p.Valid() {
			elem := p.NextElem()
			if elem == nil {
				return fmt.Errorf("pg: unexpected NULL: %q", f)
			}
			vv = append(vv, string(elem))
		}
		*v = vv
		return p.Err()
	case *[]int:
		p := newArrayParser(f[1 : len(f)-1])
		vv := make([]int, 0)
		for p.Valid() {
			elem := p.NextElem()
			if elem == nil {
				return fmt.Errorf("pg: unexpected NULL: %q", f)
			}
			n, err := strconv.ParseInt(string(elem), 10, 64)
			if err != nil {
				return err
			}
			vv = append(vv, int(n))
		}
		*v = vv
		return p.Err()
	case *[]int64:
		p := newArrayParser(f[1 : len(f)-1])
		vv := make([]int64, 0)
		for p.Valid() {
			elem := p.NextElem()
			if elem == nil {
				return fmt.Errorf("pg: unexpected NULL: %q", f)
			}
			n, err := strconv.ParseInt(string(elem), 10, 64)
			if err != nil {
				return err
			}
			vv = append(vv, n)
		}
		*v = vv
		return p.Err()
	case *map[string]string:
		p := newHstoreParser(f)
		vv := make(map[string]string)
		for p.Valid() {
			key, err := p.NextKey()
			if err != nil {
				return err
			}
			if key == nil {
				return fmt.Errorf("pg: unexpected NULL: %q", f)
			}
			value, err := p.NextValue()
			if err != nil {
				return err
			}
			if value == nil {
				return fmt.Errorf("pg: unexpected NULL: %q", f)
			}
			vv[string(key)] = string(value)
		}
		*v = vv
		return nil
	}

	v := reflect.ValueOf(dst)
	if !v.IsValid() {
		return fmt.Errorf("pg: Decode(%q)", v)
	}
	if v.Kind() != reflect.Ptr {
		return errors.New("pg: pointer expected")
	}
	v = v.Elem()

	switch v.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		n, err := strconv.ParseInt(string(f), 10, 64)
		if err != nil {
			return err
		}
		v.SetInt(n)
		return nil
	default:
		return fmt.Errorf("pg: unsupported dst type: %T", dst)
	}
}
