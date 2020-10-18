package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/go-pg/pg/v11/internal/pool"
	"github.com/go-pg/pg/v11/orm"
)

// Connect connects to a database using provided options.
//
// The returned DB is safe for concurrent use by multiple goroutines
// and maintains its own connection pool.
func Connect(opt *Options) *DB {
	opt.init()
	return newDB(
		&baseDB{
			opt:   opt,
			pool:  newConnPool(opt),
			fmter: orm.NewFormatter(),
		},
	)
}

func newDB(baseDB *baseDB) *DB {
	db := &DB{
		baseDB: baseDB.clone(),
	}
	db.baseDB.db = db
	return db
}

// DB is a database handle representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type DB struct {
	*baseDB
}

var _ orm.DB = (*DB)(nil)

func (db *DB) String() string {
	return fmt.Sprintf("DB<Addr=%q%s>", db.opt.Addr, db.fmter)
}

// Options returns read-only Options that were used to connect to the DB.
func (db *DB) Options() *Options {
	return db.opt
}

// WithTimeout returns a copy of the DB that uses d as the read/write timeout.
func (db *DB) WithTimeout(d time.Duration) *DB {
	return newDB(db.baseDB.WithTimeout(d))
}

// WithParam returns a copy of the DB that replaces the param with the value
// in queries.
func (db *DB) WithParam(param string, value interface{}) *DB {
	return newDB(db.baseDB.WithParam(param, value))
}

// Listen listens for notifications sent with NOTIFY command.
func (db *DB) Listen(ctx context.Context, channels ...string) *Listener {
	ln := &Listener{
		db: db,
	}
	ln.init()
	_ = ln.Listen(ctx, channels...)
	return ln
}

// Conn represents a single database connection rather than a pool of database
// connections. Prefer running queries from DB unless there is a specific
// need for a continuous single database connection.
//
// A Conn must call Close to return the connection to the database pool
// and may do so concurrently with a running query.
//
// After a call to Close, all operations on the connection fail.
type Conn struct {
	*baseDB
}

var _ orm.DB = (*Conn)(nil)

// Conn returns a single connection from the connection pool.
// Queries run on the same Conn will be run in the same database session.
//
// Every Conn must be returned to the database pool after use by
// calling Conn.Close.
func (db *DB) Conn() *Conn {
	return newConn(db.baseDB.withPool(pool.NewStickyConnPool(db.pool)))
}

func newConn(baseDB *baseDB) *Conn {
	conn := &Conn{
		baseDB: baseDB,
	}
	conn.baseDB.db = conn
	return conn
}

// WithTimeout returns a copy of the DB that uses d as the read/write timeout.
func (db *Conn) WithTimeout(d time.Duration) *Conn {
	return newConn(db.baseDB.WithTimeout(d))
}

// WithParam returns a copy of the DB that replaces the param with the value
// in queries.
func (db *Conn) WithParam(param string, value interface{}) *Conn {
	return newConn(db.baseDB.WithParam(param, value))
}
