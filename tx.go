package pg

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/pool"
	"github.com/go-pg/pg/orm"
)

var errTxDone = errors.New("pg: transaction has already been committed or rolled back")

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
}

var _ orm.DB = (*Tx)(nil)

// Begin starts a transaction. Most callers should use RunInTransaction instead.
func (db *baseDB) Begin() (*Tx, error) {
	tx := &Tx{
		db:  db.withPool(pool.NewSingleConnPool(db.pool)),
		ctx: db.db.Context(),
	}
	if err := tx.begin(); err != nil {
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

// Begin returns the transaction.
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

func (tx *Tx) withConn(fn func(cn *pool.Conn) error) error {
	err := tx.db.withConn(fn)
	if err == pool.ErrClosed {
		return errTxDone
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
func (tx *Tx) Exec(query interface{}, params ...interface{}) (res Result, err error) {
	err = tx.withConn(func(cn *pool.Conn) error {
		event := tx.db.queryStarted(tx, query, params, 0)
		res, err = tx.db.simpleQuery(cn, query, params...)
		tx.db.queryProcessed(res, err, event)
		return err
	})
	return res, err
}

// ExecOne is an alias for DB.ExecOne.
func (tx *Tx) ExecOne(query interface{}, params ...interface{}) (Result, error) {
	res, err := tx.Exec(query, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Query is an alias for DB.Query.
func (tx *Tx) Query(model interface{}, query interface{}, params ...interface{}) (res Result, err error) {
	err = tx.withConn(func(cn *pool.Conn) error {
		event := tx.db.queryStarted(tx, query, params, 0)
		res, err = tx.db.simpleQueryData(cn, model, query, params...)
		tx.db.queryProcessed(res, err, event)
		return err
	})
	if err != nil {
		return nil, err
	}

	if mod := res.Model(); mod != nil && res.RowsReturned() > 0 {
		if err = mod.AfterQuery(tx); err != nil {
			return res, err
		}
	}

	return res, err
}

// QueryOne is an alias for DB.QueryOne.
func (tx *Tx) QueryOne(model interface{}, query interface{}, params ...interface{}) (Result, error) {
	mod, err := orm.NewModel(model)
	if err != nil {
		return nil, err
	}

	res, err := tx.Query(mod, query, params...)
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

// Delete forces delete of the model with deleted_at column.
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
	err = tx.withConn(func(cn *pool.Conn) error {
		res, err = tx.db.copyFrom(cn, r, query, params...)
		return err
	})
	return res, err
}

// CopyTo is an alias for DB.CopyTo.
func (tx *Tx) CopyTo(w io.Writer, query interface{}, params ...interface{}) (res Result, err error) {
	err = tx.withConn(func(cn *pool.Conn) error {
		res, err = tx.db.copyTo(cn, w, query, params...)
		return err
	})
	return res, err
}

func (tx *Tx) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return tx.db.FormatQuery(dst, query, params...)
}

func (tx *Tx) begin() error {
	_, err := tx.Exec("BEGIN")
	if err != nil {
		tx.close(err)
	}
	return err
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	_, err := tx.Exec("COMMIT")
	tx.close(err)
	return err
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() error {
	_, err := tx.Exec("ROLLBACK")
	tx.close(err)
	return err
}

func (tx *Tx) close(lastErr error) {
	tx.stmtsMu.Lock()
	defer tx.stmtsMu.Unlock()

	for _, stmt := range tx.stmts {
		_ = stmt.Close()
	}
	tx.stmts = nil
	_ = tx.db.Close()
}

func (tx *Tx) Context() context.Context {
	return tx.ctx
}
