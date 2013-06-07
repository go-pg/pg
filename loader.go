package pg

import (
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

func decode(dst interface{}, f []byte) error {
	switch v := dst.(type) {
	case *bool:
		if string(f) == "t" {
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
		if len(f) > 0 {
			tm, err := time.Parse(timeFormat, string(f))
			if err != nil {
				return err
			}
			*v = tm.UTC()
		}
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
	case *map[string]string:
		p := newHstoreParser(f)
		vv := make(map[string]string)
		for p.Valid() {
			key := p.NextKey()
			if key == nil {
				return fmt.Errorf("pg: unexpected NULL: %q", f)
			}
			value := p.NextValue()
			if value == nil {
				return fmt.Errorf("pg: unexpected NULL: %q", f)
			}
			vv[string(key)] = string(value)
		}
		*v = vv
		return nil
	}
	return fmt.Errorf("pg: unsupported destination type: %T", dst)
}

type structLoader struct {
	v     reflect.Value
	indxs map[string][]int
	cols  []string
}

func newStructLoader(dst interface{}, cols []string) (*structLoader, error) {
	v := reflect.ValueOf(dst)
	if !v.IsValid() {
		return nil, errors.New("pg: Decode(" + v.String() + ")")
	}
	if v.Kind() != reflect.Ptr {
		return nil, errors.New("pg: pointer expected")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("pg: pointer to a struct expected, got %v", v.Kind())
	}
	indxs := tinfoMap.Indexes(v.Type())
	return &structLoader{
		v:     v,
		indxs: indxs,
		cols:  cols,
	}, nil
}

func (l *structLoader) Load(i int, b []byte) error {
	name := l.cols[i]
	indx := l.indxs[name]
	if indx == nil {
		return fmt.Errorf("pg: can not map field %q", name)
	}
	return decode(l.v.FieldByIndex(indx).Addr().Interface(), b)
}

type valuesLoader struct {
	values []interface{}
}

func LoadInto(values ...interface{}) Loader {
	return &valuesLoader{values}
}

func (l *valuesLoader) Load(i int, b []byte) error {
	return decode(l.values[i], b)
}

type StringSliceLoader struct {
	Slice []string
}

func (l *StringSliceLoader) New() interface{} {
	return l
}

func (l *StringSliceLoader) Load(i int, b []byte) error {
	l.Slice = append(l.Slice, string(b))
	return nil
}
