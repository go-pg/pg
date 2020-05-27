package pg

import (
	"context"
	"io"
	"time"

	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"

	"github.com/go-pg/pg/v10/internal"
	"github.com/go-pg/pg/v10/internal/pool"
	"github.com/go-pg/pg/v10/orm"
)

type baseDB struct {
	db   orm.DB
	opt  *Options
	pool pool.Pooler

	fmter      *orm.Formatter
	queryHooks []QueryHook
}

// PoolStats contains the stats of a connection pool
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

func (db *baseDB) getConn(ctx context.Context) (*pool.Conn, error) {
	cn, err := db.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	if cn.Inited {
		return cn, nil
	}

	err = internal.WithSpan(ctx, "init_conn", func(ctx context.Context, span trace.Span) error {
		return db.initConn(ctx, cn)
	})
	if err != nil {
		db.pool.Remove(cn, err)
		// It is safe to reset SingleConnPool if conn can't be initialized.
		if p, ok := db.pool.(*pool.SingleConnPool); ok {
			_ = p.Reset()
		}
		if err := internal.Unwrap(err); err != nil {
			return nil, err
		}
		return nil, err
	}

	return cn, nil
}

func (db *baseDB) initConn(c context.Context, cn *pool.Conn) error {
	if cn.Inited {
		return nil
	}
	cn.Inited = true

	if db.opt.TLSConfig != nil {
		err := db.enableSSL(c, cn, db.opt.TLSConfig)
		if err != nil {
			return err
		}
	}

	err := db.startup(c, cn, db.opt.User, db.opt.Password, db.opt.Database, db.opt.ApplicationName)
	if err != nil {
		return err
	}

	if db.opt.OnConnect != nil {
		p := pool.NewSingleConnPool(nil)
		p.SetConn(cn)
		return db.opt.OnConnect(newConn(c, db.withPool(p)))
	}

	return nil
}

func (db *baseDB) releaseConn(cn *pool.Conn, err error) {
	if isBadConn(err, false) {
		db.pool.Remove(cn, err)
	} else {
		db.pool.Put(cn)
	}
}

func (db *baseDB) withConn(
	ctx context.Context, fn func(context.Context, *pool.Conn) error,
) error {
	return internal.WithSpan(ctx, "with_conn", func(ctx context.Context, span trace.Span) error {
		cn, err := db.getConn(ctx)
		if err != nil {
			return err
		}

		var fnDone chan struct{}
		if ctx != nil && ctx.Done() != nil {
			fnDone = make(chan struct{})
			go func() {
				select {
				case <-fnDone: // fn has finished, skip cancel
				case <-ctx.Done():
					err := db.cancelRequest(cn.ProcessID, cn.SecretKey)
					if err != nil {
						internal.Logger.Printf("cancelRequest failed: %s", err)
					}
					// Signal end of conn use.
					fnDone <- struct{}{}
				}
			}()
		}

		defer func() {
			if fnDone != nil {
				select {
				case <-fnDone: // wait for cancel to finish request
				case fnDone <- struct{}{}: // signal fn finish, skip cancel goroutine
				}
			}
			db.releaseConn(cn, err)
		}()

		err = fn(ctx, cn)
		return err
	})
}

func (db *baseDB) shouldRetry(err error) bool {
	switch err {
	case nil, context.Canceled, context.DeadlineExceeded:
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
	return db.exec(db.db.Context(), query, params...)
}

func (db *baseDB) ExecContext(c context.Context, query interface{}, params ...interface{}) (Result, error) {
	return db.exec(c, query, params...)
}

func (db *baseDB) exec(ctx context.Context, query interface{}, params ...interface{}) (Result, error) {
	wb := pool.GetWriteBuffer()
	defer pool.PutWriteBuffer(wb)

	if err := writeQueryMsg(wb, db.fmter, query, params...); err != nil {
		return nil, err
	}

	ctx, evt, err := db.beforeQuery(ctx, db.db, nil, query, params, wb.Query())
	if err != nil {
		return nil, err
	}

	var res Result
	var lastErr error
	for attempt := 0; attempt <= db.opt.MaxRetries; attempt++ {
		attempt := attempt

		lastErr = internal.WithSpan(ctx, "exec", func(ctx context.Context, span trace.Span) error {
			if attempt > 0 {
				span.SetAttributes(kv.Int("retry", attempt))

				if err := internal.Sleep(ctx, db.retryBackoff(attempt-1)); err != nil {
					return err
				}
			}

			err = db.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
				res, err = db.simpleQuery(ctx, cn, wb)
				return err
			})

			return err
		})
		if !db.shouldRetry(err) {
			break
		}
	}

	if err := db.afterQuery(ctx, evt, res, lastErr); err != nil {
		return nil, err
	}
	return res, lastErr
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *baseDB) ExecOne(query interface{}, params ...interface{}) (Result, error) {
	return db.execOne(db.db.Context(), query, params...)
}

func (db *baseDB) ExecOneContext(ctx context.Context, query interface{}, params ...interface{}) (Result, error) {
	return db.execOne(ctx, query, params...)
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
	return db.query(db.db.Context(), model, query, params...)
}

func (db *baseDB) QueryContext(c context.Context, model, query interface{}, params ...interface{}) (Result, error) {
	return db.query(c, model, query, params...)
}

func (db *baseDB) query(ctx context.Context, model, query interface{}, params ...interface{}) (Result, error) {
	wb := pool.GetWriteBuffer()
	defer pool.PutWriteBuffer(wb)

	if err := writeQueryMsg(wb, db.fmter, query, params...); err != nil {
		return nil, err
	}

	ctx, evt, err := db.beforeQuery(ctx, db.db, model, query, params, wb.Query())
	if err != nil {
		return nil, err
	}

	var res Result
	var lastErr error
	for attempt := 0; attempt <= db.opt.MaxRetries; attempt++ {
		attempt := attempt

		lastErr = internal.WithSpan(ctx, "query", func(ctx context.Context, span trace.Span) error {
			if attempt > 0 {
				span.SetAttributes(kv.Int("retry", attempt))

				if err := internal.Sleep(ctx, db.retryBackoff(attempt-1)); err != nil {
					return err
				}
			}

			err = db.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
				res, err = db.simpleQueryData(ctx, cn, model, wb)
				return err
			})

			return err
		})
		if !db.shouldRetry(lastErr) {
			break
		}
	}

	if err := db.afterQuery(ctx, evt, res, lastErr); err != nil {
		return nil, err
	}
	return res, lastErr
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *baseDB) QueryOne(model, query interface{}, params ...interface{}) (Result, error) {
	return db.queryOne(db.db.Context(), model, query, params...)
}

func (db *baseDB) QueryOneContext(
	ctx context.Context, model, query interface{}, params ...interface{},
) (Result, error) {
	return db.queryOne(ctx, model, query, params...)
}

func (db *baseDB) queryOne(ctx context.Context, model, query interface{}, params ...interface{}) (Result, error) {
	res, err := db.QueryContext(ctx, model, query, params...)
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
	c := db.db.Context()
	err = db.withConn(c, func(c context.Context, cn *pool.Conn) error {
		res, err = db.copyFrom(c, cn, r, query, params...)
		return err
	})
	return res, err
}

// TODO: don't get/put conn in the pool
func (db *baseDB) copyFrom(
	ctx context.Context, cn *pool.Conn, r io.Reader, query interface{}, params ...interface{},
) (res Result, err error) {
	var evt *QueryEvent

	wb := pool.GetWriteBuffer()
	defer pool.PutWriteBuffer(wb)

	if err := writeQueryMsg(wb, db.fmter, query, params...); err != nil {
		return nil, err
	}

	var model interface{}
	if len(params) > 0 {
		model, _ = params[len(params)-1].(orm.TableModel)
	}

	ctx, evt, err = db.beforeQuery(ctx, db.db, model, query, params, wb.Query())
	if err != nil {
		return nil, err
	}

	defer func() {
		if afterQueryErr := db.afterQuery(ctx, evt, res, err); afterQueryErr != nil {
			err = afterQueryErr
		}
	}()

	err = cn.WithWriter(ctx, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db.fmter, query, params...)
	})
	if err != nil {
		return nil, err
	}

	err = cn.WithReader(ctx, db.opt.ReadTimeout, readCopyInResponse)
	if err != nil {
		return nil, err
	}

	for {
		err = cn.WithWriter(ctx, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
			return writeCopyData(wb, r)
		})
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	err = cn.WithWriter(ctx, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeCopyDone(wb)
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = cn.WithReader(ctx, db.opt.ReadTimeout, func(rd *pool.BufReader) error {
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
	c := db.db.Context()
	err = db.withConn(c, func(c context.Context, cn *pool.Conn) error {
		res, err = db.copyTo(c, cn, w, query, params...)
		return err
	})
	return res, err
}

func (db *baseDB) copyTo(
	c context.Context, cn *pool.Conn, w io.Writer, query interface{}, params ...interface{},
) (res Result, err error) {
	var evt *QueryEvent

	wb := pool.GetWriteBuffer()
	defer pool.PutWriteBuffer(wb)

	if err := writeQueryMsg(wb, db.fmter, query, params...); err != nil {
		return nil, err
	}

	var model interface{}
	if len(params) > 0 {
		model, _ = params[len(params)-1].(orm.TableModel)
	}

	c, evt, err = db.beforeQuery(c, db.db, model, query, params, wb.Query())
	if err != nil {
		return nil, err
	}

	defer func() {
		if afterQueryErr := db.afterQuery(c, evt, res, err); afterQueryErr != nil {
			err = afterQueryErr
		}
	}()

	err = cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeQueryMsg(wb, db.fmter, query, params...)
	})
	if err != nil {
		return nil, err
	}

	err = cn.WithReader(c, db.opt.ReadTimeout, func(rd *pool.BufReader) error {
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

func (db *baseDB) Formatter() orm.QueryFormatter {
	return db.fmter
}

func (db *baseDB) cancelRequest(processID, secretKey int32) error {
	c := context.TODO()

	cn, err := db.pool.NewConn(c)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.pool.CloseConn(cn)
	}()

	return cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeCancelRequestMsg(wb, processID, secretKey)
		return nil
	})
}

func (db *baseDB) simpleQuery(
	c context.Context, cn *pool.Conn, wb *pool.WriteBuffer,
) (*result, error) {
	if err := cn.WriteBuffer(c, db.opt.WriteTimeout, wb); err != nil {
		return nil, err
	}

	var res *result
	if err := cn.WithReader(c, db.opt.ReadTimeout, func(rd *pool.BufReader) error {
		var err error
		res, err = readSimpleQuery(rd)
		return err
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *baseDB) simpleQueryData(
	c context.Context, cn *pool.Conn, model interface{}, wb *pool.WriteBuffer,
) (*result, error) {
	if err := cn.WriteBuffer(c, db.opt.WriteTimeout, wb); err != nil {
		return nil, err
	}

	var res *result
	if err := cn.WithReader(c, db.opt.ReadTimeout, func(rd *pool.BufReader) error {
		var err error
		res, err = readSimpleQueryData(c, rd, model)
		return err
	}); err != nil {
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

func (db *baseDB) prepare(
	c context.Context, cn *pool.Conn, q string,
) (string, [][]byte, error) {
	name := cn.NextID()
	err := cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeParseDescribeSyncMsg(wb, name, q)
		return nil
	})
	if err != nil {
		return "", nil, err
	}

	var columns [][]byte
	err = cn.WithReader(c, db.opt.ReadTimeout, func(rd *pool.BufReader) error {
		columns, err = readParseDescribeSync(rd)
		return err
	})
	if err != nil {
		return "", nil, err
	}

	return name, columns, nil
}

func (db *baseDB) closeStmt(c context.Context, cn *pool.Conn, name string) error {
	err := cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeCloseMsg(wb, name)
		writeFlushMsg(wb)
		return nil
	})
	if err != nil {
		return err
	}

	err = cn.WithReader(c, db.opt.ReadTimeout, readCloseCompleteMsg)
	return err
}
