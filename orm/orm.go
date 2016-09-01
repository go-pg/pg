package orm

import (
	"context"

	"gopkg.in/pg.v4/types"
)

// ColumnScanner is used to scan column values.
type ColumnScanner interface {
	// Scan assigns a column value from a row.
	//
	// An error should be returned if the value can not be stored
	// without loss of information.
	ScanColumn(colIdx int, colName string, b []byte) error
}

// Collection is a set of models mapped to database rows.
type Collection interface {
	// NewModel returns ColumnScanner that is used to scan columns
	// from the current row.
	NewModel() ColumnScanner

	// AddModel adds ColumnScanner to the Collection.
	AddModel(ColumnScanner) error
}

type QueryAppender interface {
	AppendQuery(dst []byte, params ...interface{}) ([]byte, error)
}

type QueryFormatter interface {
	FormatQuery(dst []byte, query string, params ...interface{}) []byte
}

type dber interface {
	Context() context.Context
	Exec(q interface{}, params ...interface{}) (*types.Result, error)
	ExecOne(q interface{}, params ...interface{}) (*types.Result, error)
	Query(coll, query interface{}, params ...interface{}) (*types.Result, error)
	QueryOne(model, query interface{}, params ...interface{}) (*types.Result, error)
	QueryFormatter
}
