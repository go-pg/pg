package pg

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

var (
	Discard Loader = discardLoader{}

	_ Loader = &structLoader{}
	_ Loader = &valuesLoader{}
	_ Loader = &Strings{}
	_ Loader = &Ints{}
)

type Loader interface {
	Load(colIdx int, colName string, b []byte) error
}

//------------------------------------------------------------------------------

type discardLoader struct{}

func (discardLoader) Load(colIdx int, colName string, b []byte) error {
	return nil
}

//------------------------------------------------------------------------------

type structLoader struct {
	v     *reflect.Value
	indxs map[string][]int
}

func newStructLoader(v *reflect.Value) *structLoader {
	return &structLoader{
		v:     v,
		indxs: tinfoMap.Indexes(v.Type()),
	}
}

func (l *structLoader) Load(colIdx int, colName string, b []byte) error {
	indx := l.indxs[colName]
	if indx == nil {
		return fmt.Errorf("pg: can not map field %q", colName)
	}
	return Decode(l.v.FieldByIndex(indx).Addr().Interface(), b)
}

//------------------------------------------------------------------------------

type valuesLoader struct {
	values []interface{}
}

func LoadInto(values ...interface{}) Loader {
	return &valuesLoader{values}
}

func (l *valuesLoader) Load(colIdx int, colName string, b []byte) error {
	return Decode(l.values[colIdx], b)
}

//------------------------------------------------------------------------------

type Strings []string

func (strings *Strings) New() interface{} {
	return strings
}

func (strings *Strings) Load(colIdx int, colName string, b []byte) error {
	*strings = append(*strings, string(b))
	return nil
}

func (strings Strings) Append(dst []byte) []byte {
	if len(strings) <= 0 {
		return dst
	}

	for _, s := range strings {
		dst = appendPgString(dst, s)
		dst = append(dst, ',')
	}
	dst = dst[:len(dst)-1]
	return dst
}

//------------------------------------------------------------------------------

type Ints []int64

func (ints *Ints) New() interface{} {
	return ints
}

func (ints *Ints) Load(colIdx int, colName string, b []byte) error {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*ints = append(*ints, n)
	return nil
}

func (ints Ints) Append(dst []byte) []byte {
	if len(ints) <= 0 {
		return dst
	}

	for _, v := range ints {
		dst = strconv.AppendInt(dst, v, 10)
		dst = append(dst, ',')
	}
	dst = dst[:len(dst)-1]
	return dst
}

//------------------------------------------------------------------------------

func newLoader(dst interface{}) (Loader, error) {
	v := reflect.ValueOf(dst)
	if !v.IsValid() {
		return nil, errors.New("pg: Decode(" + v.String() + ")")
	}
	if v.Kind() != reflect.Ptr {
		return nil, errors.New("pg: pointer expected")
	}
	v = v.Elem()
	switch v.Kind() {
	case reflect.Struct:
		return newStructLoader(&v), nil
	}
	return nil, fmt.Errorf("pg: unsupported type %v", v.Type().String())
}
