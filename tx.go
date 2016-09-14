package pg

import (
	"io"
	"os"

	"gopkg.in/pg.v4/internal/pool"
	"gopkg.in/pg.v4/orm"
	"gopkg.in/pg.v4/types"
)

// When true Tx does not issue BEGIN, COMMIT, and ROLLBACK.
// Also underlying database connection is immediately returned to the pool.
// This is primarily useful for running your database tests in transaction.
// singleTx can be enabled with GO_PG_NO_TX environment variable.
var noTx bool

func init() {
	_, noTx = os.LookupEnv("GO_PG_NO_TX")
}

// Tx is an in-progress database transaction.
//
// A transaction must end with a call to Commit or Rollback.
//
// After a call to Commit or Rollback, all operations on the transaction fail
// with ErrTxDone.
//
// The statements prepared for a transaction by calling the transaction's
// Prepare or Stmt methods are closed by the call to Commit or Rollback.
type Tx struct {
	db *DB
	cn *pool.Conn

	stmts []*Stmt
}

var _ orm.DB = (*Tx)(nil)

// Begin starts a transaction. Most callers should use RunInTransaction instead.
func (db *DB) Begin() (*Tx, error) {
	tx := &Tx{
		db: db,
	}

	if !noTx {
		cn, err := db.conn()
		if err != nil {
			return nil, err
		}
		tx.cn = cn
	}

	if err := tx.begin(); err != nil {
		return nil, err
	}

	return tx, nil
}

// RunInTransaction runs a function in a transaction. If function
// returns an error transaction is rollbacked, otherwise transaction
// is committed.
func (db *DB) RunInTransaction(fn func(*Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (tx *Tx) conn() (*pool.Conn, error) {
	if noTx {
		return tx.db.conn()
	}
	if tx.cn == nil {
		return nil, errTxDone
	}
	tx.cn.SetReadTimeout(tx.db.opt.ReadTimeout)
	tx.cn.SetWriteTimeout(tx.db.opt.WriteTimeout)
	return tx.cn, nil
}

func (tx *Tx) freeConn(cn *pool.Conn, err error) {
	if noTx {
		_ = tx.db.freeConn(cn, err)
	}
}

// Stmt returns a transaction-specific prepared statement from an existing statement.
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
	cn, err := tx.conn()
	if err != nil {
		return nil, err
	}

	stmt, err := prepare(tx.db, cn, q)
	tx.freeConn(cn, err)
	if err != nil {
		return nil, err
	}

	stmt.inTx = true
	tx.stmts = append(tx.stmts, stmt)

	return stmt, nil
}

// Exec executes a query with the given parameters in a transaction.
func (tx *Tx) Exec(query interface{}, params ...interface{}) (*types.Result, error) {
	cn, err := tx.conn()
	if err != nil {
		return nil, err
	}

	res, err := simpleQuery(cn, query, params...)
	tx.freeConn(cn, err)
	return res, err
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (tx *Tx) ExecOne(query interface{}, params ...interface{}) (*types.Result, error) {
	res, err := tx.Exec(query, params...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res)
}

// Query executes a query with the given parameters in a transaction.
func (tx *Tx) Query(model interface{}, query interface{}, params ...interface{}) (*types.Result, error) {
	cn, err := tx.conn()
	if err != nil {
		return nil, err
	}

	res, mod, err := simpleQueryData(cn, model, query, params...)
	tx.freeConn(cn, err)
	if err != nil {
		return nil, err
	}

	if mod != nil {
		if err = mod.AfterQuery(tx); err != nil {
			return res, err
		}
	}

	return res, err
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (tx *Tx) QueryOne(model interface{}, query interface{}, params ...interface{}) (*types.Result, error) {
	mod, err := orm.NewModel(model)
	if err != nil {
		return nil, err
	}

	res, err := tx.Query(mod, query, params...)
	if err != nil {
		return nil, err
	}

	return assertOneAffected(res)
}

// Model returns new query for the model.
func (tx *Tx) Model(model ...interface{}) *orm.Query {
	return orm.NewQuery(tx, model...)
}

// Select selects the model by primary key.
func (tx *Tx) Select(model interface{}) error {
	return orm.Select(tx, model)
}

// Create inserts the model updating primary keys if they are empty.
func (tx *Tx) Create(model ...interface{}) error {
	return orm.Create(tx, model...)
}

// Update updates the model by primary key.
func (tx *Tx) Update(model interface{}) error {
	return orm.Update(tx, model)
}

// Delete deletes the model by primary key.
func (tx *Tx) Delete(model interface{}) error {
	return orm.Delete(tx, model)
}

func (tx *Tx) begin() error {
	if noTx {
		return nil
	}

	_, err := tx.Exec("BEGIN")
	return err
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	if noTx {
		return nil
	}

	_, err := tx.Exec("COMMIT")
	tx.close(err)
	return err
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() error {
	if noTx {
		return nil
	}

	_, err := tx.Exec("ROLLBACK")
	tx.close(err)
	return err
}

func (tx *Tx) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return tx.db.FormatQuery(dst, query, params...)
}

func (tx *Tx) close(lastErr error) error {
	if tx.cn == nil {
		return errTxDone
	}

	for _, stmt := range tx.stmts {
		_ = stmt.Close()
	}
	tx.stmts = nil

	err := tx.db.freeConn(tx.cn, lastErr)
	tx.cn = nil

	return err
}

// CopyFrom copies data from reader to a table.
func (tx *Tx) CopyFrom(r io.Reader, query string, params ...interface{}) (*types.Result, error) {
	cn, err := tx.conn()
	if err != nil {
		return nil, err
	}

	res, err := copyFrom(cn, r, query, params...)
	tx.freeConn(cn, err)
	return res, err
}
