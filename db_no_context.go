// +build !go1.7

package pg

import (
	"github.com/go-pg/pg/internal/pool"
	"github.com/go-pg/pg/orm"
)

// DB is a database handle representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type DB struct {
	opt   *Options
	pool  *pool.ConnPool
	fmter orm.Formatter

	queryProcessedHooks []queryProcessedHook
}
