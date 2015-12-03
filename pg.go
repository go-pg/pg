package pg

import (
	"strconv"

	"github.com/go-pg/pg/types"
	"gopkg.in/pg.v3/orm"
)

var (
	// Discard can be used with Query and QueryOne to discard rows.
	Discard discardLoader
)

// Q is a QueryAppender that represents safe SQL query.
type Q string

var _ types.QueryAppender = Q("")

func (q Q) AppendQuery(dst []byte) []byte {
	return append(dst, string(q)...)
}

func (q Q) AppendRawQuery(dst []byte) []byte {
	return q.AppendQuery(dst)
}

//------------------------------------------------------------------------------

// F is a QueryAppender that represents SQL field, e.g. table or column name.
type F string

var _ types.QueryAppender = F("")

func (f F) AppendQuery(dst []byte) []byte {
	return types.AppendField(dst, string(f))
}

//------------------------------------------------------------------------------

type discardLoader struct{}

var _ orm.Collection = (*discardLoader)(nil)
var _ orm.ColumnLoader = (*discardLoader)(nil)

func (l discardLoader) NewRecord() interface{} {
	return l
}

func (discardLoader) LoadColumn(colIdx int, colName string, b []byte) error {
	return nil
}

//------------------------------------------------------------------------------

type valuesLoader struct {
	values []interface{}
}

var _ orm.ColumnLoader = (*valuesLoader)(nil)

// LoadInto returns ColumnLoader that copies the columns in the
// row into the values.
//
// TODO(vmihailenco): rename to Scan
func LoadInto(values ...interface{}) orm.ColumnLoader {
	return &valuesLoader{values}
}

func (l *valuesLoader) LoadColumn(colIdx int, _ string, b []byte) error {
	return types.Decode(l.values[colIdx], b)
}

//------------------------------------------------------------------------------

type Strings []string

var _ orm.Collection = (*Strings)(nil)
var _ orm.ColumnLoader = (*Strings)(nil)

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
		dst = types.AppendString(dst, s, true)
		dst = append(dst, ',')
	}
	dst = dst[:len(dst)-1]
	return dst
}

//------------------------------------------------------------------------------

type Ints []int64

var _ orm.Collection = (*Ints)(nil)
var _ orm.ColumnLoader = (*Ints)(nil)

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

var _ orm.Collection = (*IntSet)(nil)
var _ orm.ColumnLoader = (*IntSet)(nil)

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
