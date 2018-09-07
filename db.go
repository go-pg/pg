package pg

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/pool"
	"github.com/go-pg/pg/orm"
)

// Connect connects to a database using provided options.
//
// The returned DB is safe for concurrent use by multiple goroutines
// and maintains its own connection pool.
func Connect(opt *Options) *DB {
	opt.init()
	return &DB{
		opt:  opt,
		pool: newConnPool(opt),
	}
}

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

var _ orm.DB = (*DB)(nil)

func (db *DB) String() string {
	return fmt.Sprintf("DB<Addr=%q%s>", db.opt.Addr, db.fmter)
}

// Options returns read-only Options that were used to connect to the DB.
func (db *DB) Options() *Options {
	return db.opt
}

// Context returns DB context.
func (db *DB) Context() context.Context {
	if db.ctx != nil {
		return db.ctx
	}
	return context.Background()
}

// WithContext returns a copy of the DB that uses the ctx.
func (db *DB) WithContext(ctx context.Context) *DB {
	return &DB{
		opt:   db.opt,
		pool:  db.pool,
		fmter: db.fmter,

		queryProcessedHooks: copyQueryProcessedHooks(db.queryProcessedHooks),

		ctx: ctx,
	}
}

// WithTimeout returns a copy of the DB that uses d as the read/write timeout.
func (db *DB) WithTimeout(d time.Duration) *DB {
	newopt := *db.opt
	newopt.ReadTimeout = d
	newopt.WriteTimeout = d

	return &DB{
		opt:   &newopt,
		pool:  db.pool,
		fmter: db.fmter,

		queryProcessedHooks: copyQueryProcessedHooks(db.queryProcessedHooks),

		ctx: db.ctx,
	}
}

// WithParam returns a copy of the DB that replaces the param with the value
// in queries.
func (db *DB) WithParam(param string, value interface{}) *DB {
	return &DB{
		opt:   db.opt,
		pool:  db.pool,
		fmter: db.fmter.WithParam(param, value),

		queryProcessedHooks: copyQueryProcessedHooks(db.queryProcessedHooks),

		ctx: db.ctx,
	}
}

// Param returns value for the param.
func (db *DB) Param(param string) interface{} {
	return db.fmter.Param(param)
}

type PoolStats pool.Stats

// PoolStats returns connection pool stats.
func (db *DB) PoolStats() *PoolStats {
	stats := db.pool.Stats()
	return (*PoolStats)(stats)
}

func (db *DB) retryBackoff(retry int) time.Duration {
	return internal.RetryBackoff(retry, db.opt.MinRetryBackoff, db.opt.MaxRetryBackoff)
}

func (db *DB) conn() (*pool.Conn, error) {
	cn, err := db.pool.Get()
	if err != nil {
		return nil, err
	}

	if cn.InitedAt.IsZero() {
		cn.InitedAt = time.Now()
		err = db.initConn(cn)
		if err != nil {
			db.pool.Remove(cn)
			return nil, err
		}
	}

	return cn, nil
}

func (db *DB) initConn(cn *pool.Conn) error {
	if db.opt.TLSConfig != nil {
		err := db.enableSSL(cn, db.opt.TLSConfig)
		if err != nil {
			return err
		}
	}

	err := db.startup(cn, db.opt.User, db.opt.Password, db.opt.Database, db.opt.ApplicationName)
	if err != nil {
		return err
	}

	if db.opt.OnConnect != nil {
		dbConn := &DB{
			opt:   db.opt,
			pool:  pool.NewSingleConnPool(cn),
			fmter: db.fmter,
		}
		return db.opt.OnConnect(dbConn)
	}

	return nil
}

func (db *DB) freeConn(cn *pool.Conn, err error) {
	if !isBadConn(err, false) {
		db.pool.Put(cn)
	} else {
		db.pool.Remove(cn)
	}
}

func (db *DB) shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if pgerr, ok := err.(Error); ok {
		switch pgerr.Field('C') {
		case "40001": // serialization_failure
			return true
		case "53300": // too_many_connections
			return true
		case "55000": // attempted to delete invisible tuple
			return true
		case "57014": // statement_timeout
			return db.opt.RetryStatementTimeout
		default:
			return false
		}
	}
	return isNetworkError(err)
}

// Close closes the database client, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (db *DB) Close() error {
	return db.pool.Close()
}

// Exec executes a query ignoring returned rows. The params are for any
// placeholders in the query.
func (db *DB) Exec(query interface{}, params ...interface{}) (res orm.Result, err error) {
	for attempt := 0; attempt <= db.opt.MaxRetries; attempt++ {
		var cn *pool.Conn

		if attempt >= 1 {
			time.Sleep(db.retryBackoff(attempt - 1))
		}

		cn, err = db.conn()
		if err != nil {
			continue
		}

		start := time.Now()
		res, err = db.simpleQuery(cn, query, params...)
		db.freeConn(cn, err)
		db.queryProcessed(db, start, query, params, attempt, res, err)

		if !db.shouldRetry(err) {
			break
		}
	}
	return res, err
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *DB) ExecOne(query interface{}, params ...interface{}) (orm.Result, error) {
	res, err := db.Exec(query, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The params are for any placeholders in the query.
func (db *DB) Query(model, query interface{}, params ...interface{}) (res orm.Result, err error) {
	for attempt := 0; attempt <= db.opt.MaxRetries; attempt++ {
		var cn *pool.Conn

		if attempt >= 1 {
			time.Sleep(db.retryBackoff(attempt - 1))
		}

		cn, err = db.conn()
		if err != nil {
			continue
		}

		start := time.Now()
		res, err = db.simpleQueryData(cn, model, query, params...)
		db.freeConn(cn, err)
		db.queryProcessed(db, start, query, params, attempt, res, err)

		if !db.shouldRetry(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	if mod := res.Model(); mod != nil && res.RowsReturned() > 0 {
		if err = mod.AfterQuery(db); err != nil {
			return res, err
		}
	}

	return res, nil
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *DB) QueryOne(model, query interface{}, params ...interface{}) (orm.Result, error) {
	res, err := db.Query(model, query, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Listen listens for notifications sent with NOTIFY command.
func (db *DB) Listen(channels ...string) *Listener {
	ln := &Listener{
		db: db,
	}
	ln.init()
	_ = ln.Listen(channels...)
	return ln
}

// CopyFrom copies data from reader to a table.
func (db *DB) CopyFrom(r io.Reader, query interface{}, params ...interface{}) (orm.Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	res, err := db.copyFrom(cn, r, query, params...)
	db.freeConn(cn, err)
	return res, err
}

func (db *DB) copyFrom(cn *pool.Conn, r io.Reader, query interface{}, params ...interface{}) (orm.Result, error) {
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db, query, params...)
	})
	if err != nil {
		return nil, err
	}

	err = cn.WithReader(db.opt.ReadTimeout, func(rd *pool.Reader) error {
		return readCopyInResponse(rd)
	})
	if err != nil {
		return nil, err
	}

	for {
		err = cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
			return writeCopyData(wb, r)
		})
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	err = cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeCopyDone(wb)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var res orm.Result
	err = cn.WithReader(db.opt.ReadTimeout, func(rd *pool.Reader) error {
		res, err = readReadyForQuery(rd)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// CopyTo copies data from a table to writer.
func (db *DB) CopyTo(w io.Writer, query interface{}, params ...interface{}) (orm.Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	res, err := db.copyTo(cn, w, query, params...)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	db.pool.Put(cn)
	return res, nil
}

func (db *DB) copyTo(cn *pool.Conn, w io.Writer, query interface{}, params ...interface{}) (orm.Result, error) {
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db, query, params...)
	})
	if err != nil {
		return nil, err
	}

	var res orm.Result
	err = cn.WithReader(db.opt.ReadTimeout, func(rd *pool.Reader) error {
		err := readCopyOutResponse(rd)
		if err != nil {
			return err
		}

		res, err = readCopyData(rd, w)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Model returns new query for the model.
func (db *DB) Model(model ...interface{}) *orm.Query {
	return orm.NewQuery(db, model...)
}

// Select selects the model by primary key.
func (db *DB) Select(model interface{}) error {
	return orm.Select(db, model)
}

// Insert inserts the model updating primary keys if they are empty.
func (db *DB) Insert(model ...interface{}) error {
	return orm.Insert(db, model...)
}

// Update updates the model by primary key.
func (db *DB) Update(model interface{}) error {
	return orm.Update(db, model)
}

// Delete deletes the model by primary key.
func (db *DB) Delete(model interface{}) error {
	return orm.Delete(db, model)
}

// Delete forces delete of the model with deleted_at column.
func (db *DB) ForceDelete(model interface{}) error {
	return orm.ForceDelete(db, model)
}

// CreateTable creates table for the model. It recognizes following field tags:
//   - notnull - sets NOT NULL constraint.
//   - unique - sets UNIQUE constraint.
//   - default:value - sets default value.
func (db *DB) CreateTable(model interface{}, opt *orm.CreateTableOptions) error {
	return orm.CreateTable(db, model, opt)
}

// DropTable drops table for the model.
func (db *DB) DropTable(model interface{}, opt *orm.DropTableOptions) error {
	return orm.DropTable(db, model, opt)
}

func (db *DB) CreateComposite(model interface{}, opt *orm.CreateCompositeOptions) error {
	return orm.CreateComposite(db, model, opt)
}

func (db *DB) DropComposite(model interface{}, opt *orm.DropCompositeOptions) error {
	return orm.DropComposite(db, model, opt)
}

func (db *DB) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return db.fmter.Append(dst, query, params...)
}

func (db *DB) cancelRequest(processId, secretKey int32) error {
	cn, err := db.pool.NewConn()
	if err != nil {
		return err
	}

	err = cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeCancelRequestMsg(wb, processId, secretKey)
		return nil
	})
	if err != nil {
		return err
	}

	cn.Close()
	return nil
}

func (db *DB) simpleQuery(
	cn *pool.Conn, query interface{}, params ...interface{},
) (orm.Result, error) {
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db, query, params...)
	})
	if err != nil {
		return nil, err
	}

	var res orm.Result
	err = cn.WithReader(db.opt.ReadTimeout, func(rd *pool.Reader) error {
		res, err = readSimpleQuery(rd)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (db *DB) simpleQueryData(
	cn *pool.Conn, model, query interface{}, params ...interface{},
) (orm.Result, error) {
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db, query, params...)
	})
	if err != nil {
		return nil, err
	}

	var res orm.Result
	err = cn.WithReader(db.opt.ReadTimeout, func(rd *pool.Reader) error {
		res, err = readSimpleQueryData(rd, model)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}
