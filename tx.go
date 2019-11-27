package pg

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/go-pg/pg/v9/internal"
	"github.com/go-pg/pg/v9/internal/pool"
	"github.com/go-pg/pg/v9/orm"
)

// ErrTxDone is returned by any operation that is performed on a transaction
// that has already been committed or rolled back.
var ErrTxDone = errors.New("pg: transaction has already been committed or rolled back")

// Tx is an in-progress database transaction. It is safe for concurrent use
// by multiple goroutines.
//
// A transaction must end with a call to Commit or Rollback.
//
// After a call to Commit or Rollback, all operations on the transaction fail
// with ErrTxDone.
//
// The statements prepared for a transaction by calling the transaction's
// Prepare or Stmt methods are closed by the call to Commit or Rollback.
type Tx struct {
	db  *baseDB
	ctx context.Context

	stmtsMu sync.Mutex
	stmts   []*Stmt

	_closed int32
}

var _ orm.DB = (*Tx)(nil)

// Context returns the context.Context of the transaction
func (tx *Tx) Context() context.Context {
	return tx.ctx
}

// Begin starts a transaction. Most callers should use RunInTransaction instead.
func (db *baseDB) Begin() (*Tx, error) {
	tx := &Tx{
		db:  db.withPool(pool.NewSingleConnPool(db.pool)),
		ctx: db.db.Context(),
	}

	err := tx.begin(tx.ctx)
	if err != nil {
		tx.close()
		return nil, err
	}

	return tx, nil
}

// RunInTransaction runs a function in a transaction. If function
// returns an error transaction is rollbacked, otherwise transaction
// is committed.
func (db *baseDB) RunInTransaction(fn func(*Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	return tx.RunInTransaction(fn)
}

// Begin returns current transaction. It does not start new transaction.
func (tx *Tx) Begin() (*Tx, error) {
	return tx, nil
}

// RunInTransaction runs a function in the transaction. If function
// returns an error transaction is rollbacked, otherwise transaction
// is committed.
func (tx *Tx) RunInTransaction(fn func(*Tx) error) error {
	defer func() {
		if err := recover(); err != nil {
			_ = tx.Rollback()
			panic(err)
		}
	}()
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (tx *Tx) withConn(c context.Context, fn func(context.Context, *pool.Conn) error) error {
	err := tx.db.withConn(c, fn)
	if err == pool.ErrClosed {
		return ErrTxDone
	}
	return err
}

// Stmt returns a transaction-specific prepared statement
// from an existing statement.
func (tx *Tx) Stmt(stmt *Stmt) *Stmt {
	stmt, err := tx.Prepare(stmt.q)
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
func (tx *Tx) Prepare(q string) (*Stmt, error) {
	tx.stmtsMu.Lock()
	defer tx.stmtsMu.Unlock()

	stmt, err := prepareStmt(tx.db, q)
	if err != nil {
		return nil, err
	}
	tx.stmts = append(tx.stmts, stmt)

	return stmt, nil
}

// Exec is an alias for DB.Exec.
func (tx *Tx) Exec(query interface{}, params ...interface{}) (Result, error) {
	return tx.exec(tx.ctx, query, params...)
}

// ExecContext acts like Exec but additionally receives a context
func (tx *Tx) ExecContext(c context.Context, query interface{}, params ...interface{}) (Result, error) {
	return tx.exec(c, query, params...)
}

func (tx *Tx) exec(c context.Context, query interface{}, params ...interface{}) (Result, error) {
	c, evt, err := tx.db.beforeQuery(c, tx, nil, query, params)
	if err != nil {
		return nil, err
	}

	var res Result
	lastErr := tx.withConn(c, func(c context.Context, cn *pool.Conn) error {
		res, err = tx.db.simpleQuery(c, cn, query, params...)
		return err
	})

	if err := tx.db.afterQuery(c, evt, res, lastErr); err != nil {
		return nil, err
	}
	return res, lastErr
}

// ExecOne is an alias for DB.ExecOne.
func (tx *Tx) ExecOne(query interface{}, params ...interface{}) (Result, error) {
	return tx.execOne(tx.ctx, query, params...)
}

// ExecOneContext acts like ExecOne but additionally receives a context
func (tx *Tx) ExecOneContext(c context.Context, query interface{}, params ...interface{}) (Result, error) {
	return tx.execOne(c, query, params...)
}

func (tx *Tx) execOne(c context.Context, query interface{}, params ...interface{}) (Result, error) {
	res, err := tx.ExecContext(c, query, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Query is an alias for DB.Query.
func (tx *Tx) Query(model interface{}, query interface{}, params ...interface{}) (Result, error) {
	return tx.query(tx.ctx, model, query, params...)
}

// QueryContext acts like Query but additionally receives a context
func (tx *Tx) QueryContext(
	c context.Context,
	model interface{},
	query interface{},
	params ...interface{},
) (Result, error) {
	return tx.query(c, model, query, params...)
}

func (tx *Tx) query(
	c context.Context,
	model interface{},
	query interface{},
	params ...interface{},
) (Result, error) {
	c, evt, err := tx.db.beforeQuery(c, tx, model, query, params)
	if err != nil {
		return nil, err
	}

	var res *result
	lastErr := tx.withConn(c, func(c context.Context, cn *pool.Conn) error {
		res, err = tx.db.simpleQueryData(c, cn, model, query, params...)
		return err
	})

	if err := tx.db.afterQuery(c, evt, res, lastErr); err != nil {
		return nil, err
	}
	return res, lastErr
}

// QueryOne is an alias for DB.QueryOne.
func (tx *Tx) QueryOne(model interface{}, query interface{}, params ...interface{}) (Result, error) {
	return tx.queryOne(tx.ctx, model, query, params...)
}

// QueryOneContext acts like QueryOne but additionally receives a context
func (tx *Tx) QueryOneContext(
	c context.Context,
	model interface{},
	query interface{},
	params ...interface{},
) (Result, error) {
	return tx.queryOne(c, model, query, params...)
}

func (tx *Tx) queryOne(
	c context.Context,
	model interface{},
	query interface{},
	params ...interface{},
) (Result, error) {
	mod, err := orm.NewModel(model)
	if err != nil {
		return nil, err
	}

	res, err := tx.QueryContext(c, mod, query, params...)
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

// ModelContext acts like Model but additionally receives a context
func (tx *Tx) ModelContext(c context.Context, model ...interface{}) *orm.Query {
	return orm.NewQueryContext(c, tx, model...)
}

// Select is an alias for DB.Select.
func (tx *Tx) Select(model interface{}) error {
	return orm.Select(tx, model)
}

// Insert is an alias for DB.Insert.
func (tx *Tx) Insert(model ...interface{}) error {
	return orm.Insert(tx, model...)
}

// Update is an alias for DB.Update.
func (tx *Tx) Update(model interface{}) error {
	return orm.Update(tx, model)
}

// Delete is an alias for DB.Delete.
func (tx *Tx) Delete(model interface{}) error {
	return orm.Delete(tx, model)
}

// ForceDelete forces the deletion of the model with deleted_at column.
func (tx *Tx) ForceDelete(model interface{}) error {
	return orm.ForceDelete(tx, model)
}

// CreateTable is an alias for DB.CreateTable.
func (tx *Tx) CreateTable(model interface{}, opt *orm.CreateTableOptions) error {
	return orm.CreateTable(tx, model, opt)
}

// DropTable is an alias for DB.DropTable.
func (tx *Tx) DropTable(model interface{}, opt *orm.DropTableOptions) error {
	return orm.DropTable(tx, model, opt)
}

// CopyFrom is an alias for DB.CopyFrom.
func (tx *Tx) CopyFrom(r io.Reader, query interface{}, params ...interface{}) (res Result, err error) {
	err = tx.withConn(tx.ctx, func(c context.Context, cn *pool.Conn) error {
		res, err = tx.db.copyFrom(c, cn, r, query, params...)
		return err
	})
	return res, err
}

// CopyTo is an alias for DB.CopyTo.
func (tx *Tx) CopyTo(w io.Writer, query interface{}, params ...interface{}) (res Result, err error) {
	err = tx.withConn(tx.ctx, func(c context.Context, cn *pool.Conn) error {
		res, err = tx.db.copyTo(c, cn, w, query, params...)
		return err
	})
	return res, err
}

// Formatter is an alias for DB.Formatter
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

			err := tx.db.pool.(*pool.SingleConnPool).Reset()
			if err != nil {
				return err
			}
		}

		_, lastErr = tx.ExecContext(ctx, "BEGIN")
		if !tx.db.shouldRetry(lastErr) {
			break
		}
	}
	return lastErr
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	_, err := tx.Exec("COMMIT")
	tx.close()
	return err
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() error {
	_, err := tx.Exec("ROLLBACK")
	tx.close()
	return err
}

// Close calls Rollback if the tx has not already beed committed or rolled back.
func (tx *Tx) Close() error {
	if tx.closed() {
		return nil
	}
	return tx.Rollback()
}

func (tx *Tx) close() {
	if !atomic.CompareAndSwapInt32(&tx._closed, 0, 1) {
		return
	}

	tx.stmtsMu.Lock()
	defer tx.stmtsMu.Unlock()

	for _, stmt := range tx.stmts {
		_ = stmt.Close()
	}
	tx.stmts = nil
	_ = tx.db.Close()
}

func (tx *Tx) closed() bool {
	return atomic.LoadInt32(&tx._closed) == 1
}
