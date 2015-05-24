package pg

import (
	"reflect"
	"strconv"
)

var (
	Discard = discardLoader{}
)

//------------------------------------------------------------------------------

type singleRecordCollection struct {
	v interface{}
}

var _ Collection = &singleRecordCollection{}

func (f *singleRecordCollection) NewRecord() interface{} {
	return f.v
}

//------------------------------------------------------------------------------

type discardLoader struct{}

var _ Collection = &discardLoader{}
var _ ColumnLoader = &discardLoader{}

func (l discardLoader) NewRecord() interface{} {
	return l
}

func (discardLoader) LoadColumn(colIdx int, colName string, b []byte) error {
	return nil
}

//------------------------------------------------------------------------------

type structLoader struct {
	v      reflect.Value // reflect.Struct
	fields map[string]*pgValue
}

var _ ColumnLoader = &structLoader{}

func newStructLoader(v reflect.Value) *structLoader {
	return &structLoader{
		v:      v,
		fields: structs.Fields(v.Type()),
	}
}

func (l *structLoader) LoadColumn(colIdx int, colName string, b []byte) error {
	field, ok := l.fields[colName]
	if !ok {
		return errorf("pg: cannot map field %q", colName)
	}
	return field.DecodeValue(l.v, b)
}

//------------------------------------------------------------------------------

type valuesLoader struct {
	values []interface{}
}

var _ ColumnLoader = &valuesLoader{}

func LoadInto(values ...interface{}) ColumnLoader {
	return &valuesLoader{values}
}

func (l *valuesLoader) LoadColumn(colIdx int, _ string, b []byte) error {
	return Decode(l.values[colIdx], b)
}

//------------------------------------------------------------------------------

type Strings []string

var _ Collection = &Strings{}
var _ ColumnLoader = &Strings{}

func (strings *Strings) NewRecord() interface{} {
	return strings
}

func (strings *Strings) LoadColumn(colIdx int, _ string, b []byte) error {
	*strings = append(*strings, string(b))
	return nil
}

func (strings Strings) AppendQuery(dst []byte) []byte {
	if len(strings) <= 0 {
		return dst
	}

	for _, s := range strings {
		dst = appendString(dst, s)
		dst = append(dst, ',')
	}
	dst = dst[:len(dst)-1]
	return dst
}

//------------------------------------------------------------------------------

type Ints []int64

var _ Collection = &Ints{}
var _ ColumnLoader = &Ints{}

func (ints *Ints) NewRecord() interface{} {
	return ints
}

func (ints *Ints) LoadColumn(colIdx int, colName string, b []byte) error {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*ints = append(*ints, n)
	return nil
}

func (ints Ints) AppendQuery(dst []byte) []byte {
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

type IntSet map[int64]struct{}

var _ Collection = &IntSet{}
var _ ColumnLoader = &IntSet{}

func (set IntSet) NewRecord() interface{} {
	return set
}

func (set IntSet) LoadColumn(colIdx int, colName string, b []byte) error {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	set[n] = struct{}{}
	return nil
}

//------------------------------------------------------------------------------

func NewColumnLoader(dst interface{}) (ColumnLoader, error) {
	v := reflect.ValueOf(dst)
	if !v.IsValid() {
		return nil, errorf("pg: Decode(nil)")
	}
	if v.Kind() != reflect.Ptr {
		return nil, errorf("pg: Decode(nonsettable %T)", dst)
	}
	vv := v.Elem()
	switch vv.Kind() {
	case reflect.Struct:
		return newStructLoader(vv), nil
	}
	return nil, errorf("pg: Decode(unsupported %T)", dst)
}
