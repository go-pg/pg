package pg

import (
	"log"
	"strconv"

	"gopkg.in/pg.v4/internal"
	"gopkg.in/pg.v4/orm"
	"gopkg.in/pg.v4/types"
)

// Discard is used with Query and QueryOne to discard rows.
var Discard orm.Discard

// Scan returns ColumnScanner that copies the columns in the
// row into the values.
func Scan(values ...interface{}) orm.ColumnScanner {
	return orm.Scan(values...)
}

// Q returns a ValueAppender that represents safe SQL query.
func Q(query string, params ...interface{}) types.Q {
	return orm.Q(query, params...)
}

// F returns a ValueAppender that represents SQL identifier,
// e.g. table or column name.
func F(field string, params ...interface{}) types.F {
	return orm.F(field, params...)
}

// Array returns an Array type that represents PostgreSQL array of any type.
func Array(v interface{}) *types.Array {
	return types.NewArray(v)
}

func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}

//------------------------------------------------------------------------------

type Strings []string

var _ orm.Model = (*Strings)(nil)
var _ types.ValueAppender = (*Strings)(nil)

func (strings *Strings) NewModel() orm.ColumnScanner {
	return strings
}

func (Strings) AddModel(_ orm.ColumnScanner) error {
	return nil
}

func (strings *Strings) ScanColumn(colIdx int, _ string, b []byte) error {
	*strings = append(*strings, string(b))
	return nil
}

func (strings Strings) AppendValue(dst []byte, quote int) ([]byte, error) {
	if len(strings) <= 0 {
		return dst, nil
	}

	for _, s := range strings {
		dst = types.AppendString(dst, s, 1)
		dst = append(dst, ',')
	}
	dst = dst[:len(dst)-1]
	return dst, nil
}

//------------------------------------------------------------------------------

type Ints []int64

var _ orm.Model = (*Ints)(nil)
var _ types.ValueAppender = (*Ints)(nil)

func (ints *Ints) NewModel() orm.ColumnScanner {
	return ints
}

func (Ints) AddModel(_ orm.ColumnScanner) error {
	return nil
}

func (ints *Ints) ScanColumn(colIdx int, colName string, b []byte) error {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*ints = append(*ints, n)
	return nil
}

func (ints Ints) AppendValue(dst []byte, quote int) ([]byte, error) {
	if len(ints) <= 0 {
		return dst, nil
	}

	for _, v := range ints {
		dst = strconv.AppendInt(dst, v, 10)
		dst = append(dst, ',')
	}
	dst = dst[:len(dst)-1]
	return dst, nil
}

//------------------------------------------------------------------------------

type IntSet map[int64]struct{}

var _ orm.Model = (*IntSet)(nil)

func (set *IntSet) NewModel() orm.ColumnScanner {
	return set
}

func (IntSet) AddModel(_ orm.ColumnScanner) error {
	return nil
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
