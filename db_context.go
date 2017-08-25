// +build go1.7

package pg

import (
	"context"

	"github.com/go-pg/pg/internal/pool"
	"github.com/go-pg/pg/orm"
)

// DB is a database handle representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type DB struct {
	opt   *Options
	pool  pool.Pooler
	fmter orm.Formatter

	queryProcessedHooks []queryProcessedHook

	ctx context.Context
}

func (db *DB) Context() context.Context {
	if db.ctx != nil {
		return db.ctx
	}
	return context.Background()
}

func (db *DB) WithContext(ctx context.Context) *DB {
	if ctx == nil {
		panic("nil context")
	}
	cp := *db
	cp.ctx = ctx
	return &cp
}
