// +build !go1.7

package pg

import (
	"time"

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
}

// WithTimeout returns a DB that uses d as the read/write timeout.
func (db *DB) WithTimeout(d time.Duration) *DB {
	newopt := *db.opt
	newopt.ReadTimeout = d
	newopt.WriteTimeout = d

	return &DB{
		opt:   &newopt,
		pool:  db.pool,
		fmter: db.fmter,

		queryProcessedHooks: copyQueryProcessedHooks(db.queryProcessedHooks),
	}
}

// WithParam returns a DB that replaces the param with the value in queries.
func (db *DB) WithParam(param string, value interface{}) *DB {
	return &DB{
		opt:   db.opt,
		pool:  db.pool,
		fmter: db.fmter.WithParam(param, value),

		queryProcessedHooks: copyQueryProcessedHooks(db.queryProcessedHooks),
	}
}
