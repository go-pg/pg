package pg

import (
	"log"
	"strconv"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/orm"
	"github.com/go-pg/pg/types"
)

// Discard is used with Query and QueryOne to discard rows.
var Discard orm.Discard

// Model returns new query for the optional model.
func Model(model ...interface{}) *orm.Query {
	return orm.NewQuery(nil, model...)
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
//    Where("id IN (?)", pg.In([]int{1, 2, 3}))
func In(slice interface{}) types.ValueAppender {
	return types.In(slice)
}

// Array accepts a slice and returns a wrapper for working with PostgreSQL
// array data type.
//
// For struct fields you can use array tag:
//
//    Emails  []string `pg:",array"`
func Array(v interface{}) *types.Array {
	return types.NewArray(v)
}

// Hstore accepts a map and returns a wrapper for working with hstore data type.
// Supported map types are:
//   - map[string]string
//
// For struct fields you can use hstore tag:
//
//    Attrs map[string]string `pg:",hstore"`
func Hstore(v interface{}) *types.Hstore {
	return types.NewHstore(v)
}

func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}

//------------------------------------------------------------------------------

type Strings []string

var _ orm.Model = (*Strings)(nil)
var _ types.ValueAppender = (*Strings)(nil)

func (strings *Strings) Reset() error {
	if s := *strings; len(s) > 0 {
		*strings = s[:0]
	}
	return nil
}

func (strings *Strings) NewModel() orm.ColumnScanner {
	return strings
}

func (Strings) AddModel(_ orm.ColumnScanner) error {
	return nil
}

func (Strings) AfterQuery(_ orm.DB) error {
	return nil
}

func (Strings) AfterSelect(_ orm.DB) error {
	return nil
}

func (Strings) BeforeInsert(_ orm.DB) error {
	return nil
}

func (Strings) AfterInsert(_ orm.DB) error {
	return nil
}

func (Strings) BeforeUpdate(_ orm.DB) error {
	return nil
}

func (Strings) AfterUpdate(_ orm.DB) error {
	return nil
}

func (Strings) BeforeDelete(_ orm.DB) error {
	return nil
}

func (Strings) AfterDelete(_ orm.DB) error {
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

func (ints *Ints) Reset() error {
	if s := *ints; len(s) > 0 {
		*ints = s[:0]
	}
	return nil
}

func (ints *Ints) NewModel() orm.ColumnScanner {
	return ints
}

func (Ints) AddModel(_ orm.ColumnScanner) error {
	return nil
}

func (Ints) AfterQuery(_ orm.DB) error {
	return nil
}

func (Ints) AfterSelect(_ orm.DB) error {
	return nil
}

func (Ints) BeforeInsert(_ orm.DB) error {
	return nil
}

func (Ints) AfterInsert(_ orm.DB) error {
	return nil
}

func (Ints) BeforeUpdate(_ orm.DB) error {
	return nil
}

func (Ints) AfterUpdate(_ orm.DB) error {
	return nil
}

func (Ints) BeforeDelete(_ orm.DB) error {
	return nil
}

func (Ints) AfterDelete(_ orm.DB) error {
	return nil
}

func (ints *Ints) ScanColumn(colIdx int, colName string, b []byte) error {
	n, err := strconv.ParseInt(internal.BytesToString(b), 10, 64)
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

func (set *IntSet) Reset() error {
	if len(*set) > 0 {
		*set = make(map[int64]struct{})
	}
	return nil
}

func (set *IntSet) NewModel() orm.ColumnScanner {
	return set
}

func (IntSet) AddModel(_ orm.ColumnScanner) error {
	return nil
}

func (IntSet) AfterQuery(_ orm.DB) error {
	return nil
}

func (IntSet) AfterSelect(_ orm.DB) error {
	return nil
}

func (IntSet) BeforeInsert(_ orm.DB) error {
	return nil
}

func (IntSet) AfterInsert(_ orm.DB) error {
	return nil
}

func (IntSet) BeforeUpdate(_ orm.DB) error {
	return nil
}

func (IntSet) AfterUpdate(_ orm.DB) error {
	return nil
}

func (IntSet) BeforeDelete(_ orm.DB) error {
	return nil
}

func (IntSet) AfterDelete(_ orm.DB) error {
	return nil
}

func (setptr *IntSet) ScanColumn(colIdx int, colName string, b []byte) error {
	set := *setptr
	if set == nil {
		*setptr = make(IntSet)
		set = *setptr
	}

	n, err := strconv.ParseInt(internal.BytesToString(b), 10, 64)
	if err != nil {
		return err
	}
	set[n] = struct{}{}
	return nil
}
