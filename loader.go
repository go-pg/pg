package pg

import (
	"reflect"
	"strconv"
)

var (
	// Discard can be used with Query and QueryOne to discard rows.
	Discard discardLoader
)

//------------------------------------------------------------------------------

type singleRecordCollection struct {
	record interface{}
	len    int
}

var _ Collection = (*singleRecordCollection)(nil)

func (coll *singleRecordCollection) NewRecord() interface{} {
	coll.len++
	return coll.record
}

func (coll *singleRecordCollection) Len() int {
	return coll.len
}

//------------------------------------------------------------------------------

type discardLoader struct{}

var _ Collection = (*discardLoader)(nil)
var _ ColumnLoader = (*discardLoader)(nil)

func (l discardLoader) NewRecord() interface{} {
	return l
}

func (discardLoader) LoadColumn(colIdx int, colName string, b []byte) error {
	return nil
}

//------------------------------------------------------------------------------

type structLoader struct {
	v  reflect.Value // reflect.Struct
	fs map[string]*field
}

var _ ColumnLoader = (*structLoader)(nil)

func newStructLoader(v reflect.Value) *structLoader {
	return &structLoader{
		v:  v,
		fs: structs.Fields(v.Type()).Table,
	}
}

func (l *structLoader) LoadColumn(colIdx int, colName string, b []byte) error {
	field, ok := l.fs[colName]
	if !ok {
		return errorf("pg: cannot find field %q in %s", colName, l.v.Type())
	}
	return field.DecodeValue(l.v, b)
}

//------------------------------------------------------------------------------

type valuesLoader struct {
	values []interface{}
}

var _ ColumnLoader = (*valuesLoader)(nil)

// LoadInto returns ColumnLoader that copies the columns in the
// row into the values.
//
// TODO(vmihailenco): rename to Scan
func LoadInto(values ...interface{}) ColumnLoader {
	return &valuesLoader{values}
}

func (l *valuesLoader) LoadColumn(colIdx int, _ string, b []byte) error {
	return Decode(l.values[colIdx], b)
}

//------------------------------------------------------------------------------

type Strings []string

var _ Collection = (*Strings)(nil)
var _ ColumnLoader = (*Strings)(nil)

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
		dst = appendString(dst, s, true)
		dst = append(dst, ',')
	}
	dst = dst[:len(dst)-1]
	return dst
}

//------------------------------------------------------------------------------

type Ints []int64

var _ Collection = (*Ints)(nil)
var _ ColumnLoader = (*Ints)(nil)

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

var _ Collection = (*IntSet)(nil)
var _ ColumnLoader = (*IntSet)(nil)

func (set *IntSet) NewRecord() interface{} {
	return set
}

func (setptr *IntSet) LoadColumn(colIdx int, colName string, b []byte) error {
	set := *setptr
	if set == nil {
		*setptr = make(IntSet)
		set = *setptr
	}

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

//------------------------------------------------------------------------------

type sliceCollection struct {
	v reflect.Value // reflect.Slice
}

var _ Collection = (*sliceCollection)(nil)

func (coll *sliceCollection) newValue() reflect.Value {
	switch coll.v.Type().Elem().Kind() {
	case reflect.Ptr:
		elem := reflect.New(coll.v.Type().Elem().Elem())
		coll.v.Set(reflect.Append(coll.v, elem))
		return elem.Elem()
	case reflect.Struct:
		elem := reflect.New(coll.v.Type().Elem()).Elem()
		coll.v.Set(reflect.Append(coll.v, elem))
		elem = coll.v.Index(coll.v.Len() - 1)
		return elem
	default:
		panic("not reached")
	}
}

func (coll *sliceCollection) NewRecord() interface{} {
	return newStructLoader(coll.newValue())
}

func newCollection(vi interface{}) (*sliceCollection, error) {
	v := reflect.ValueOf(vi)
	if !v.IsValid() {
		return nil, errorf("pg: Decode(nil)")
	}
	if v.Kind() != reflect.Ptr {
		return nil, errorf("pg: Decode(nonsettable %T)", vi)
	}

	return newCollectionValue(v.Elem())
}

func newCollectionValue(v reflect.Value) (*sliceCollection, error) {
	if v.Kind() != reflect.Slice {
		return nil, errorf("pg: Decode(unsupported %s)", v.Type())
	}

	elem := v.Type().Elem()
	switch elem.Kind() {
	case reflect.Struct:
		return &sliceCollection{v}, nil
	case reflect.Ptr:
		if elem.Elem().Kind() != reflect.Struct {
			return nil, errorf("pg: Decode(unsupported %s)", v.Type())
		}
		return &sliceCollection{v}, nil
	default:
		return nil, errorf("pg: Decode(unsupported %s)", v.Type())
	}
}
