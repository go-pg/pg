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

// Q returns a ValueAppender that represents safe SQL query.
func Q(s string) types.Q {
	return types.Q(s)
}

// F returns a ValueAppender that represents SQL identifier,
// e.g. table or column name.
func F(s string) types.F {
	return types.F(s)
}

var Scan = orm.Scan

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
