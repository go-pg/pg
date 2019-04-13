package pg

import (
	"context"
	"io"
	"time"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/pool"
	"github.com/go-pg/pg/orm"
)

type baseDB struct {
	db   orm.DB
	opt  *Options
	pool pool.Pooler

	fmter      orm.Formatter
	queryHooks []QueryHook
}

type PoolStats pool.Stats

// PoolStats returns connection pool stats.
func (db *baseDB) PoolStats() *PoolStats {
	stats := db.pool.Stats()
	return (*PoolStats)(stats)
}

func (db *baseDB) clone() *baseDB {
	return &baseDB{
		db:   db.db,
		opt:  db.opt,
		pool: db.pool,

		fmter:      db.fmter,
		queryHooks: copyQueryHooks(db.queryHooks),
	}
}

func (db *baseDB) withPool(p pool.Pooler) *baseDB {
	cp := db.clone()
	cp.pool = p
	return cp
}

func (db *baseDB) WithTimeout(d time.Duration) *baseDB {
	newopt := *db.opt
	newopt.ReadTimeout = d
	newopt.WriteTimeout = d

	cp := db.clone()
	cp.opt = &newopt
	return cp
}

func (db *baseDB) WithParam(param string, value interface{}) *baseDB {
	cp := db.clone()
	cp.fmter = db.fmter.WithParam(param, value)
	return cp
}

// Param returns value for the param.
func (db *baseDB) Param(param string) interface{} {
	return db.fmter.Param(param)
}

func (db *baseDB) retryBackoff(retry int) time.Duration {
	return internal.RetryBackoff(retry, db.opt.MinRetryBackoff, db.opt.MaxRetryBackoff)
}

func (db *baseDB) conn() (*pool.Conn, error) {
	cn, err := db.pool.Get()
	if err != nil {
		return nil, err
	}

	err = db.initConn(cn)
	if err != nil {
		db.pool.Remove(cn)
		return nil, err
	}

	return cn, nil
}

func (db *baseDB) initConn(cn *pool.Conn) error {
	if cn.Inited {
		return nil
	}
	cn.Inited = true

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
		p := pool.NewSingleConnPool(nil)
		p.SetConn(cn)
		return db.opt.OnConnect(newConn(db.withPool(p), nil))
	}

	return nil
}

func (db *baseDB) freeConn(cn *pool.Conn, err error) {
	if !isBadConn(err, false) {
		db.pool.Put(cn)
	} else {
		db.pool.Remove(cn)
	}
}

func (db *baseDB) withConn(c context.Context, fn func(cn *pool.Conn) error) error {
	cn, err := db.conn()
	if err != nil {
		return err
	}

	var fnDone chan struct{}
	if c != nil && c.Done() != nil {
		fnDone = make(chan struct{})
		go func() {
			select {
			case <-fnDone: // fn has finished, skip cancel
			case <-c.Done():
				_ = db.cancelRequest(cn.ProcessId, cn.SecretKey)
				// Indicate end of conn use
				fnDone <- struct{}{}
			}
		}()
	}

	defer func() {
		if fnDone != nil {
			select {
			case <-fnDone: // Wait for cancel to finish request
			case fnDone <- struct{}{}: // Indicate fn finish, skip cancel goroutine
			}
		}
		db.freeConn(cn, err)
	}()

	err = fn(cn)
	return err
}

func (db *baseDB) shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if pgerr, ok := err.(Error); ok {
		switch pgerr.Field('C') {
		case "40001", // serialization_failure
			"53300", // too_many_connections
			"55000": // attempted to delete invisible tuple
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
func (db *baseDB) Close() error {
	return db.pool.Close()
}

// Exec executes a query ignoring returned rows. The params are for any
// placeholders in the query.
func (db *baseDB) Exec(query interface{}, params ...interface{}) (res Result, err error) {
	return db.exec(nil, query, params...)
}

func (db *baseDB) ExecContext(c context.Context, query interface{}, params ...interface{}) (Result, error) {
	return db.exec(c, query, params...)
}

func (db *baseDB) exec(c context.Context, query interface{}, params ...interface{}) (res Result, err error) {
	for attempt := 0; attempt <= db.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(db.retryBackoff(attempt - 1))
		}

		err = db.withConn(c, func(cn *pool.Conn) error {
			event := db.queryStarted(c, db.db, query, params, attempt)
			res, err = db.simpleQuery(cn, query, params...)
			db.queryProcessed(res, err, event)
			return err
		})
		if !db.shouldRetry(err) {
			break
		}
	}
	return res, err
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *baseDB) ExecOne(query interface{}, params ...interface{}) (Result, error) {
	return db.execOne(nil, query, params...)
}

func (db *baseDB) ExecOneContext(c context.Context, query interface{}, params ...interface{}) (Result, error) {
	return db.execOne(c, query, params...)
}

func (db *baseDB) execOne(c context.Context, query interface{}, params ...interface{}) (Result, error) {
	res, err := db.ExecContext(c, query, params...)
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
func (db *baseDB) Query(model, query interface{}, params ...interface{}) (res Result, err error) {
	return db.query(nil, model, query, params...)
}

func (db *baseDB) QueryContext(c context.Context, model, query interface{}, params ...interface{}) (Result, error) {
	return db.query(c, model, query, params...)
}

func (db *baseDB) query(c context.Context, model, query interface{}, params ...interface{}) (res Result, err error) {
	for attempt := 0; attempt <= db.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(db.retryBackoff(attempt - 1))
		}

		err = db.withConn(c, func(cn *pool.Conn) error {
			event := db.queryStarted(c, db.db, query, params, attempt)
			res, err = db.simpleQueryData(cn, model, query, params...)
			db.queryProcessed(res, err, event)
			return err
		})
		if !db.shouldRetry(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	if mod := res.Model(); mod != nil && res.RowsReturned() > 0 {
		if err = mod.AfterQuery(c, db.db); err != nil {
			return res, err
		}
	}

	return res, nil
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *baseDB) QueryOne(model, query interface{}, params ...interface{}) (Result, error) {
	return db.queryOne(nil, model, query, params...)
}

func (db *baseDB) QueryOneContext(c context.Context, model, query interface{}, params ...interface{}) (Result, error) {
	return db.queryOne(c, model, query, params...)
}

func (db *baseDB) queryOne(c context.Context, model, query interface{}, params ...interface{}) (Result, error) {
	res, err := db.QueryContext(c, model, query, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// CopyFrom copies data from reader to a table.
func (db *baseDB) CopyFrom(r io.Reader, query interface{}, params ...interface{}) (res Result, err error) {
	err = db.withConn(nil, func(cn *pool.Conn) error {
		res, err = db.copyFrom(cn, r, query, params...)
		return err
	})
	return res, err
}

func (db *baseDB) copyFrom(cn *pool.Conn, r io.Reader, query interface{}, params ...interface{}) (Result, error) {
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db, query, params...)
	})
	if err != nil {
		return nil, err
	}

	err = cn.WithReader(db.opt.ReadTimeout, func(rd *internal.BufReader) error {
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

	var res Result
	err = cn.WithReader(db.opt.ReadTimeout, func(rd *internal.BufReader) error {
		res, err = readReadyForQuery(rd)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// CopyTo copies data from a table to writer.
func (db *baseDB) CopyTo(w io.Writer, query interface{}, params ...interface{}) (res Result, err error) {
	err = db.withConn(nil, func(cn *pool.Conn) error {
		res, err = db.copyTo(cn, w, query, params...)
		return err
	})
	return res, err
}

func (db *baseDB) copyTo(cn *pool.Conn, w io.Writer, query interface{}, params ...interface{}) (Result, error) {
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db, query, params...)
	})
	if err != nil {
		return nil, err
	}

	var res Result
	err = cn.WithReader(db.opt.ReadTimeout, func(rd *internal.BufReader) error {
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
func (db *baseDB) Model(model ...interface{}) *orm.Query {
	return orm.NewQuery(db.db, model...)
}

func (db *baseDB) ModelContext(c context.Context, model ...interface{}) *orm.Query {
	return orm.NewQueryContext(c, db.db, model...)
}

// Select selects the model by primary key.
func (db *baseDB) Select(model interface{}) error {
	return orm.Select(db.db, model)
}

// Insert inserts the model updating primary keys if they are empty.
func (db *baseDB) Insert(model ...interface{}) error {
	return orm.Insert(db.db, model...)
}

// Update updates the model by primary key.
func (db *baseDB) Update(model interface{}) error {
	return orm.Update(db.db, model)
}

// Delete deletes the model by primary key.
func (db *baseDB) Delete(model interface{}) error {
	return orm.Delete(db.db, model)
}

// Delete forces delete of the model with deleted_at column.
func (db *baseDB) ForceDelete(model interface{}) error {
	return orm.ForceDelete(db.db, model)
}

// CreateTable creates table for the model. It recognizes following field tags:
//   - notnull - sets NOT NULL constraint.
//   - unique - sets UNIQUE constraint.
//   - default:value - sets default value.
func (db *baseDB) CreateTable(model interface{}, opt *orm.CreateTableOptions) error {
	return orm.CreateTable(db.db, model, opt)
}

// DropTable drops table for the model.
func (db *baseDB) DropTable(model interface{}, opt *orm.DropTableOptions) error {
	return orm.DropTable(db.db, model, opt)
}

func (db *baseDB) CreateComposite(model interface{}, opt *orm.CreateCompositeOptions) error {
	return orm.CreateComposite(db.db, model, opt)
}

func (db *baseDB) DropComposite(model interface{}, opt *orm.DropCompositeOptions) error {
	return orm.DropComposite(db.db, model, opt)
}

func (db *baseDB) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return db.fmter.Append(dst, query, params...)
}

func (db *baseDB) cancelRequest(processId, secretKey int32) error {
	cn, err := db.pool.NewConn()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.pool.CloseConn(cn)
	}()

	err = cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeCancelRequestMsg(wb, processId, secretKey)
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (db *baseDB) simpleQuery(
	cn *pool.Conn, query interface{}, params ...interface{},
) (Result, error) {
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db, query, params...)
	})
	if err != nil {
		return nil, err
	}

	var res Result
	err = cn.WithReader(db.opt.ReadTimeout, func(rd *internal.BufReader) error {
		res, err = readSimpleQuery(rd)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (db *baseDB) simpleQueryData(
	cn *pool.Conn, model, query interface{}, params ...interface{},
) (Result, error) {
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db, query, params...)
	})
	if err != nil {
		return nil, err
	}

	var res Result
	err = cn.WithReader(db.opt.ReadTimeout, func(rd *internal.BufReader) error {
		res, err = readSimpleQueryData(rd, model)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Prepare creates a prepared statement for later queries or
// executions. Multiple queries or executions may be run concurrently
// from the returned statement.
func (db *baseDB) Prepare(q string) (*Stmt, error) {
	return prepareStmt(db.withPool(pool.NewSingleConnPool(db.pool)), q)
}

func (db *baseDB) prepare(cn *pool.Conn, q string) (string, [][]byte, error) {
	name := cn.NextId()
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeParseDescribeSyncMsg(wb, name, q)
		return nil
	})
	if err != nil {
		return "", nil, err
	}

	var columns [][]byte
	err = cn.WithReader(db.opt.ReadTimeout, func(rd *internal.BufReader) error {
		columns, err = readParseDescribeSync(rd)
		return err
	})
	if err != nil {
		return "", nil, err
	}

	return name, columns, nil
}

func (db *baseDB) closeStmt(cn *pool.Conn, name string) error {
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeCloseMsg(wb, name)
		writeFlushMsg(wb)
		return nil
	})
	if err != nil {
		return err
	}

	err = cn.WithReader(db.opt.ReadTimeout, func(rd *internal.BufReader) error {
		return readCloseCompleteMsg(rd)
	})
	return err
}
