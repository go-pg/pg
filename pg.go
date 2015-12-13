package pg

import (
	"strconv"

	"gopkg.in/pg.v3/orm"
	"gopkg.in/pg.v3/types"
)

var (
	// Discard can be used with Query and QueryOne to discard rows.
	Discard discard
)

//------------------------------------------------------------------------------

// TODO: fix duplication (how?)

// Q is a ValueAppender that represents safe SQL query.
type Q string

var _ types.ValueAppender = Q("")

func (q Q) AppendValue(dst []byte, quote bool) []byte {
	return append(dst, string(q)...)
}

// F is a ValueAppender that represents SQL field, e.g. table or column name.
type F string

var _ types.ValueAppender = F("")

func (f F) AppendValue(dst []byte, quote bool) []byte {
	return types.AppendField(dst, string(f))
}

//------------------------------------------------------------------------------

type discard struct{}

var _ orm.Collection = (*discard)(nil)
var _ orm.ColumnScanner = (*discard)(nil)

func (l discard) NextModel() interface{} {
	return l
}

func (discard) ScanColumn(colIdx int, colName string, b []byte) error {
	return nil
}

//------------------------------------------------------------------------------

type valuesScanner struct {
	values []interface{}
}

var _ orm.ColumnScanner = (*valuesScanner)(nil)

// Scan returns ColumnScanner that copies the columns in the
// row into the values.
func Scan(values ...interface{}) orm.ColumnScanner {
	return &valuesScanner{values}
}

func (l *valuesScanner) ScanColumn(colIdx int, _ string, b []byte) error {
	return types.Decode(l.values[colIdx], b)
}

//------------------------------------------------------------------------------

type Strings []string

var _ orm.Collection = (*Strings)(nil)
var _ orm.ColumnScanner = (*Strings)(nil)
var _ types.ValueAppender = (*Strings)(nil)

func (strings *Strings) NextModel() interface{} {
	return strings
}

func (strings *Strings) ScanColumn(colIdx int, _ string, b []byte) error {
	*strings = append(*strings, string(b))
	return nil
}

func (strings Strings) AppendValue(dst []byte, quote bool) []byte {
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
var _ orm.ColumnScanner = (*Ints)(nil)
var _ types.ValueAppender = (*Ints)(nil)

func (ints *Ints) NextModel() interface{} {
	return ints
}

func (ints *Ints) ScanColumn(colIdx int, colName string, b []byte) error {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*ints = append(*ints, n)
	return nil
}

func (ints Ints) AppendValue(dst []byte, quote bool) []byte {
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
var _ orm.ColumnScanner = (*IntSet)(nil)

func (set *IntSet) NextModel() interface{} {
	return set
}

func (setptr *IntSet) ScanColumn(colIdx int, colName string, b []byte) error {
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
