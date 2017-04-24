package pg

import (
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
	pool  *pool.ConnPool
	fmter orm.Formatter

	queryProcessedHooks []queryProcessedHook
}

var _ orm.DB = (*DB)(nil)

func (db *DB) String() string {
	return fmt.Sprintf("DB<Addr=%q%s>", db.opt.Addr, db.fmter)
}

// Options returns read-only Options that were used to connect to the DB.
func (db *DB) Options() *Options {
	return db.opt
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

		queryProcessedHooks: db.queryProcessedHooks[:len(db.queryProcessedHooks):len(db.queryProcessedHooks)],
	}
}

// WithParam returns a DB that replaces the param with the value in queries.
func (db *DB) WithParam(param string, value interface{}) *DB {
	return &DB{
		opt:   db.opt,
		pool:  db.pool,
		fmter: db.fmter.WithParam(param, value),

		queryProcessedHooks: db.queryProcessedHooks[:len(db.queryProcessedHooks):len(db.queryProcessedHooks)],
	}
}

func (db *DB) conn() (*pool.Conn, error) {
	cn, _, err := db.pool.Get()
	if err != nil {
		return nil, err
	}

	cn.SetReadWriteTimeout(db.opt.ReadTimeout, db.opt.WriteTimeout)

	if cn.InitedAt.IsZero() {
		if err := db.initConn(cn); err != nil {
			_ = db.pool.Remove(cn)
			return nil, err
		}
		cn.InitedAt = time.Now()
	}

	return cn, nil
}

func (db *DB) initConn(cn *pool.Conn) error {
	if db.opt.TLSConfig != nil {
		if err := enableSSL(cn, db.opt.TLSConfig); err != nil {
			return err
		}
	}

	return startup(cn, db.opt.User, db.opt.Password, db.opt.Database)
}

func (db *DB) freeConn(cn *pool.Conn, err error) error {
	if !isBadConn(err, false) {
		return db.pool.Put(cn)
	}
	return db.pool.Remove(cn)
}

func (db *DB) shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if pgerr, ok := err.(Error); ok {
		switch pgerr.Field('C') {
		case "40001": // serialization_failure
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
	st := db.pool.Stats()
	if st.TotalConns != st.FreeConns {
		internal.Logf(
			"connection leaking detected: total_conns=%d free_conns=%d",
			st.TotalConns, st.FreeConns,
		)
	}
	return db.pool.Close()
}

// Exec executes a query ignoring returned rows. The params are for any
// placeholders in the query.
func (db *DB) Exec(query interface{}, params ...interface{}) (res orm.Result, err error) {
	for i := 0; i <= db.opt.MaxRetries; i++ {
		var cn *pool.Conn

		if i >= 1 {
			time.Sleep(internal.RetryBackoff(i - 1))
		}

		cn, err = db.conn()
		if err != nil {
			continue
		}

		start := time.Now()
		res, err = db.simpleQuery(cn, query, params...)
		db.freeConn(cn, err)
		db.queryProcessed(db, start, query, params, res, err)

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
	for i := 0; i <= db.opt.MaxRetries; i++ {
		var cn *pool.Conn

		if i >= 1 {
			time.Sleep(internal.RetryBackoff(i - 1))
		}

		cn, err = db.conn()
		if err != nil {
			continue
		}

		start := time.Now()
		res, err = db.simpleQueryData(cn, model, query, params...)
		db.freeConn(cn, err)
		db.queryProcessed(db, start, query, params, res, err)

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
	_ = ln.Listen(channels...)
	return ln
}

// CopyFrom copies data from reader to a table.
func (db *DB) CopyFrom(reader io.Reader, query interface{}, params ...interface{}) (orm.Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	res, err := db.copyFrom(cn, reader, query, params...)
	db.freeConn(cn, err)
	return res, err
}

// CopyTo copies data from a table to writer.
func (db *DB) CopyTo(writer io.Writer, query interface{}, params ...interface{}) (orm.Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	if err := writeQueryMsg(cn.Wr, db, query, params...); err != nil {
		db.pool.Put(cn)
		return nil, err
	}

	if err := cn.FlushWriter(); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	if err := readCopyOutResponse(cn); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	res, err := readCopyData(cn, writer)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	db.pool.Put(cn)
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

// CreateTable creates table for the model. It recognizes following field tags:
//   - notnull - sets NOT NULL constraint.
//   - unique - sets UNIQUE constraint.
func (db *DB) CreateTable(model interface{}, opt *orm.CreateTableOptions) error {
	_, err := orm.CreateTable(db, model, opt)
	return err
}

func (db *DB) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return db.fmter.Append(dst, query, params...)
}

func (db *DB) cancelRequest(processId, secretKey int32) error {
	cn, err := db.pool.NewConn()
	if err != nil {
		return err
	}

	writeCancelRequestMsg(cn.Wr, processId, secretKey)
	if err = cn.FlushWriter(); err != nil {
		return err
	}
	cn.Close()

	return nil
}

func (db *DB) simpleQuery(
	cn *pool.Conn, query interface{}, params ...interface{},
) (orm.Result, error) {
	if err := writeQueryMsg(cn.Wr, db, query, params...); err != nil {
		return nil, err
	}

	if err := cn.FlushWriter(); err != nil {
		return nil, err
	}

	res, err := readSimpleQuery(cn)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (db *DB) simpleQueryData(
	cn *pool.Conn, model, query interface{}, params ...interface{},
) (orm.Result, error) {
	if err := writeQueryMsg(cn.Wr, db, query, params...); err != nil {
		return nil, err
	}

	if err := cn.FlushWriter(); err != nil {
		return nil, err
	}

	res, err := readSimpleQueryData(cn, model)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (db *DB) copyFrom(cn *pool.Conn, r io.Reader, query interface{}, params ...interface{}) (orm.Result, error) {
	if err := writeQueryMsg(cn.Wr, db, query, params...); err != nil {
		return nil, err
	}

	if err := cn.FlushWriter(); err != nil {
		return nil, err
	}

	if err := readCopyInResponse(cn); err != nil {
		return nil, err
	}

	for {
		if _, err := writeCopyData(cn.Wr, r); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if err := cn.FlushWriter(); err != nil {
			return nil, err
		}
	}

	writeCopyDone(cn.Wr)
	if err := cn.FlushWriter(); err != nil {
		return nil, err
	}

	return readReadyForQuery(cn)
}
