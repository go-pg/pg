package types

import (
	"sync"

	"github.com/go-pg/pg/v9/internal"
)

type ValueScanner interface {
	ScanValue(rd Reader, n int) error
}

type ValueAppender interface {
	AppendValue(b []byte, flags int) ([]byte, error)
}

//------------------------------------------------------------------------------

// Safe represents a safe SQL query.
type Safe string

var _ ValueAppender = (*Safe)(nil)

func (q Safe) AppendValue(b []byte, flags int) ([]byte, error) {
	return append(b, q...), nil
}

//------------------------------------------------------------------------------

// DEPRECATED. Use Safe instead.
type Q string

var _ ValueAppender = (*Q)(nil)
var qWarn sync.Once

func (q Q) AppendValue(b []byte, flags int) ([]byte, error) {
	qWarn.Do(func() {
		internal.Logger.Printf("DEPRECATED: types.Q is replaced with pg.Safe")
	})
	return append(b, q...), nil
}

//------------------------------------------------------------------------------

// Ident represents a SQL identifier, e.g. table or column name.
type Ident string

var _ ValueAppender = (*Ident)(nil)

func (f Ident) AppendValue(b []byte, flags int) ([]byte, error) {
	return AppendIdent(b, string(f), flags), nil
}

//------------------------------------------------------------------------------

// DEPRECATED. Use Ident instead.
type F string

var _ ValueAppender = (*F)(nil)
var fWarn sync.Once

func (f F) AppendValue(b []byte, flags int) ([]byte, error) {
	fWarn.Do(func() {
		internal.Logger.Printf("DEPRECATED: types.F is replaced with pg.Ident")
	})
	return AppendIdent(b, string(f), flags), nil
}

//------------------------------------------------------------------------------

type Reader = internal.Reader
type BytesReader = internal.BytesReader

func NewBytesReader(buf []byte) *BytesReader {
	return internal.NewBytesReader(buf)
}
