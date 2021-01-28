package pg

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/go-pg/pg/v11/internal"
	"github.com/go-pg/pg/v11/internal/pool"
	"github.com/go-pg/pg/v11/orm"
)

// ErrTxDone is returned by any operation that is performed on a transaction
// that has already been committed or rolled back.
var ErrTxDone = errors.New("pg: transaction has already been committed or rolled back")

// Tx is an in-progress database transaction. It is safe for concurrent use
// by multiple goroutines.
//
// A transaction must end with a call to Commit, Rollback, or Close. After that,
// all operations on the transaction fail with ErrTxDone.
//
// The statements prepared for a transaction by calling the transaction's
// Prepare or Stmt methods are closed with the transaction.
type Tx struct {
	db *baseDB

	stmtsMu sync.Mutex
	stmts   []*Stmt

	_closed int32
}

var _ orm.DB = (*Tx)(nil)

// Begin starts a transaction. Most callers should use RunInTransaction instead.
func (db *baseDB) Begin(ctx context.Context) (*Tx, error) {
	tx := &Tx{
		db: db.withPool(pool.NewStickyConnPool(db.pool)),
	}

	err := tx.begin(ctx)
	if err != nil {
		tx.close(ctx)
		return nil, err
	}

	return tx, nil
}

// RunInTransaction runs a function in a transaction. If function
// returns an error transaction is rolled back, otherwise transaction
// is committed.
func (db *baseDB) RunInTransaction(
	ctx context.Context,
	fn func(ctx context.Context, tx *Tx) error,
) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	return tx.RunInTransaction(ctx, fn)
}

// Begin returns current transaction. It does not start new transaction.
func (tx *Tx) Begin(ctx context.Context) (*Tx, error) {
	return tx, nil
}

// RunInTransaction runs a function in the transaction. If function
// returns an error transaction is rolled back, otherwise transaction
// is committed.
func (tx *Tx) RunInTransaction(
	ctx context.Context,
	fn func(ctx context.Context, tx *Tx) error,
) error {
	defer func() {
		if err := recover(); err != nil {
			if err, ok := err.(error); ok {
				ctx = healthyContext(ctx, err)
			}
			if err := tx.Close(ctx); err != nil {
				internal.Logger.Printf(ctx, "tx.Close failed: %s", err)
			}
			panic(err)
		}
	}()

	if err := fn(ctx, tx); err != nil {
		if err := tx.Rollback(healthyContext(ctx, err)); err != nil {
			internal.Logger.Printf(ctx, "tx.Rollback failed: %s", err)
		}
		return err
	}
	return tx.Commit(ctx)
}

func (tx *Tx) withConn(ctx context.Context, fn func(context.Context, *pool.Conn) error) error {
	err := tx.db.withConn(ctx, fn)
	if tx.closed() && err == pool.ErrClosed {
		return ErrTxDone
	}
	return err
}

// Stmt returns a transaction-specific prepared statement
// from an existing statement.
func (tx *Tx) Stmt(ctx context.Context, stmt *Stmt) *Stmt {
	stmt, err := tx.Prepare(ctx, stmt.q)
	if err != nil {
		return &Stmt{stickyErr: err}
	}
	return stmt
}

// Prepare creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and can no longer
// be used once the transaction has been committed or rolled back.
//
// To use an existing prepared statement on this transaction, see Tx.Stmt.
func (tx *Tx) Prepare(ctx context.Context, q string) (*Stmt, error) {
	tx.stmtsMu.Lock()
	defer tx.stmtsMu.Unlock()

	db := tx.db.withPool(pool.NewStickyConnPool(tx.db.pool))
	stmt, err := prepareStmt(ctx, db, q)
	if err != nil {
		return nil, err
	}
	tx.stmts = append(tx.stmts, stmt)

	return stmt, nil
}

// Exec is an alias for DB.Exec.
func (tx *Tx) Exec(ctx context.Context, query interface{}, params ...interface{}) (Result, error) {
	wb := pool.GetWriteBuffer()
	defer pool.PutWriteBuffer(wb)

	if err := writeQueryMsg(wb, tx.db.fmter, query, params...); err != nil {
		return nil, err
	}

	ctx, evt, err := tx.db.beforeQuery(ctx, tx, nil, query, params, wb.Query())
	if err != nil {
		return nil, err
	}

	var res Result
	lastErr := tx.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		res, err = tx.db.simpleQuery(ctx, cn, wb)
		return err
	})

	if err := tx.db.afterQuery(ctx, evt, res, lastErr); err != nil {
		return nil, err
	}
	return res, lastErr
}

// ExecOne is an alias for DB.ExecOne.
func (tx *Tx) ExecOne(
	ctx context.Context, query interface{}, params ...interface{},
) (Result, error) {
	res, err := tx.Exec(ctx, query, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Query is an alias for DB.Query.
func (tx *Tx) Query(
	ctx context.Context,
	model interface{},
	query interface{},
	params ...interface{},
) (Result, error) {
	wb := pool.GetWriteBuffer()
	defer pool.PutWriteBuffer(wb)

	if err := writeQueryMsg(wb, tx.db.fmter, query, params...); err != nil {
		return nil, err
	}

	ctx, evt, err := tx.db.beforeQuery(ctx, tx, model, query, params, wb.Query())
	if err != nil {
		return nil, err
	}

	var res *result
	lastErr := tx.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		res, err = tx.db.simpleQueryData(ctx, cn, model, wb)
		return err
	})

	if err := tx.db.afterQuery(ctx, evt, res, err); err != nil {
		return nil, err
	}
	return res, lastErr
}

// QueryOne is an alias for DB.QueryOne.
func (tx *Tx) QueryOne(
	ctx context.Context,
	model interface{},
	query interface{},
	params ...interface{},
) (Result, error) {
	mod, err := orm.NewModel(model)
	if err != nil {
		return nil, err
	}

	res, err := tx.Query(ctx, mod, query, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Model is an alias for DB.Model.
func (tx *Tx) Model(model ...interface{}) *orm.Query {
	return orm.NewQuery(tx, model...)
}

// CopyFrom is an alias for DB.CopyFrom.
func (tx *Tx) CopyFrom(
	ctx context.Context, r io.Reader, query interface{}, params ...interface{},
) (res Result, err error) {
	err = tx.withConn(ctx, func(c context.Context, cn *pool.Conn) error {
		res, err = tx.db.copyFrom(c, cn, r, query, params...)
		return err
	})
	return res, err
}

// CopyTo is an alias for DB.CopyTo.
func (tx *Tx) CopyTo(
	ctx context.Context, w io.Writer, query interface{}, params ...interface{},
) (res Result, err error) {
	err = tx.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		res, err = tx.db.copyTo(ctx, cn, w, query, params...)
		return err
	})
	return res, err
}

// Formatter is an alias for DB.Formatter.
func (tx *Tx) Formatter() orm.QueryFormatter {
	return tx.db.Formatter()
}

func (tx *Tx) begin(ctx context.Context) error {
	var lastErr error
	for attempt := 0; attempt <= tx.db.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, tx.db.retryBackoff(attempt-1)); err != nil {
				return err
			}

			err := tx.db.pool.(*pool.StickyConnPool).Reset(ctx)
			if err != nil {
				return err
			}
		}

		_, lastErr = tx.Exec(ctx, "BEGIN")
		if !tx.db.shouldRetry(lastErr) {
			break
		}
	}
	return lastErr
}

// Commit commits the transaction.
func (tx *Tx) Commit(ctx context.Context) error {
	_, err := tx.Exec(ctx, "COMMIT")
	tx.close(ctx)
	return err
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback(ctx context.Context) error {
	_, err := tx.Exec(ctx, "ROLLBACK")
	tx.close(ctx)
	return err
}

// Close calls Rollback if the tx has not already been committed or rolled back.
func (tx *Tx) Close(ctx context.Context) error {
	if tx.closed() {
		return nil
	}
	return tx.Rollback(ctx)
}

func (tx *Tx) close(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&tx._closed, 0, 1) {
		return
	}

	tx.stmtsMu.Lock()
	defer tx.stmtsMu.Unlock()

	for _, stmt := range tx.stmts {
		_ = stmt.Close(ctx)
	}
	tx.stmts = nil

	_ = tx.db.Close(ctx)
}

func (tx *Tx) closed() bool {
	return atomic.LoadInt32(&tx._closed) == 1
}

func healthyContext(ctx context.Context, err error) context.Context {
	switch err {
	case context.Canceled, context.DeadlineExceeded:
		return context.Background()
	}
	return ctx
}
