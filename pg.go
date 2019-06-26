package pg

import (
	"context"
	"io"
	"log"
	"strconv"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/orm"
	"github.com/go-pg/pg/types"
)

// Discard is used with Query and QueryOne to discard rows.
var Discard orm.Discard

// NullTime is a time.Time wrapper that marshals zero time as JSON null and
// PostgreSQL NULL.
type NullTime = types.NullTime

// Model returns new query for the optional model.
func Model(model ...interface{}) *orm.Query {
	return orm.NewQuery(nil, model...)
}

// ModelContext returns a new query for the optional model with a context
func ModelContext(c context.Context, model ...interface{}) *orm.Query {
	return orm.NewQueryContext(c, nil, model...)
}

// Scan returns ColumnScanner that copies the columns in the
// row into the values.
func Scan(values ...interface{}) orm.ColumnScanner {
	return orm.Scan(values...)
}

// Q replaces any placeholders found in the query.
func Q(query string, params ...interface{}) types.ValueAppender {
	return orm.Q(query, params...)
}

// F quotes a SQL identifier such as a table or column name replacing any
// placeholders found in the field.
func F(field string) types.ValueAppender {
	return types.F(field)
}

// In accepts a slice and returns a wrapper that can be used with PostgreSQL
// IN operator:
//
//    Where("id IN (?)", pg.In([]int{1, 2, 3, 4}))
//
// produces
//
//    WHERE id IN (1, 2, 3, 4)
func In(slice interface{}) types.ValueAppender {
	return types.InSlice(slice)
}

// InMulti accepts multiple values and returns a wrapper that can be used
// with PostgreSQL IN operator:
//
//    Where("(id1, id2) IN (?)", pg.InMulti([]int{1, 2}, []int{3, 4}))
//
// produces
//
//    WHERE (id1, id2) IN ((1, 2), (3, 4))
func InMulti(values ...interface{}) types.ValueAppender {
	return types.InMulti(values...)
}

// Array accepts a slice and returns a wrapper for working with PostgreSQL
// array data type.
//
// For struct fields you can use array tag:
//
//    Emails  []string `sql:",array"`
func Array(v interface{}) *types.Array {
	return types.NewArray(v)
}

// Hstore accepts a map and returns a wrapper for working with hstore data type.
// Supported map types are:
//   - map[string]string
//
// For struct fields you can use hstore tag:
//
//    Attrs map[string]string `sql:",hstore"`
func Hstore(v interface{}) *types.Hstore {
	return types.NewHstore(v)
}

// SetLogger sets the logger to the given one
func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}

//------------------------------------------------------------------------------

// Strings is a typealias for a slice of strings
type Strings []string

var _ orm.HooklessModel = (*Strings)(nil)
var _ types.ValueAppender = (*Strings)(nil)

// Init initializes the Strings slice
func (strings *Strings) Init() error {
	if s := *strings; len(s) > 0 {
		*strings = s[:0]
	}
	return nil
}

// NewModel returns `Strings` as orm.ColumnScanner
func (strings *Strings) NewModel() orm.ColumnScanner {
	return strings
}

// AddModel ...
func (Strings) AddModel(_ orm.ColumnScanner) error {
	return nil
}

// ScanColumn scans the columns and appends them to `strings`
func (strings *Strings) ScanColumn(colIdx int, _ string, rd types.Reader, n int) error {
	b := make([]byte, n)
	_, err := io.ReadFull(rd, b)
	if err != nil {
		return err
	}

	*strings = append(*strings, internal.BytesToString(b))
	return nil
}

// AppendValue appends the values from `strings` to the given byte slice
func (strings Strings) AppendValue(dst []byte, quote int) ([]byte, error) {
	if len(strings) == 0 {
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

// Ints is a typealias for a slice of int64 values
type Ints []int64

var _ orm.HooklessModel = (*Ints)(nil)
var _ types.ValueAppender = (*Ints)(nil)

// Init initializes the Int slice
func (ints *Ints) Init() error {
	if s := *ints; len(s) > 0 {
		*ints = s[:0]
	}
	return nil
}

// NewModel returns `Ints` as orm.ColumnScanner
func (ints *Ints) NewModel() orm.ColumnScanner {
	return ints
}

// AddModel ...
func (Ints) AddModel(_ orm.ColumnScanner) error {
	return nil
}

// ScanColumn scans the columns and appends them to `ints`
func (ints *Ints) ScanColumn(colIdx int, colName string, rd types.Reader, n int) error {
	num, err := types.ScanInt64(rd, n)
	if err != nil {
		return err
	}

	*ints = append(*ints, num)
	return nil
}

// AppendValue appends the values from `ints` to the given byte slice
func (ints Ints) AppendValue(dst []byte, quote int) ([]byte, error) {
	if len(ints) == 0 {
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

// IntSet is a set of int64 values
type IntSet map[int64]struct{}

var _ orm.HooklessModel = (*IntSet)(nil)

// Init initializes the IntSet
func (set *IntSet) Init() error {
	if len(*set) > 0 {
		*set = make(map[int64]struct{})
	}
	return nil
}

// NewModel returns `Ints` as orm.ColumnScanner
func (set *IntSet) NewModel() orm.ColumnScanner {
	return set
}

// AddModel ...
func (IntSet) AddModel(_ orm.ColumnScanner) error {
	return nil
}

// ScanColumn scans the columns and appends them to `IntSet`
func (set *IntSet) ScanColumn(colIdx int, colName string, rd types.Reader, n int) error {
	num, err := types.ScanInt64(rd, n)
	if err != nil {
		return err
	}

	setVal := *set
	if setVal == nil {
		*set = make(IntSet)
		setVal = *set
	}

	setVal[num] = struct{}{}
	return nil
}
