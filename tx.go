package pg

import (
	"io"
	"os"

	"gopkg.in/pg.v4/internal/pool"
	"gopkg.in/pg.v4/types"
)

// When true Tx does not issue BEGIN, COMMIT, and ROLLBACK.
// It is primarily useful for testing and can be enabled with
// GO_PG_NO_TX environment variable.
var noTx bool

func init() {
	noTx = os.Getenv("GO_PG_NO_TX") != ""
}

// Not thread-safe.
type Tx struct {
	db  *DB
	_cn *pool.Conn

	err  error
	done bool
}

// Begin starts a transaction. Most callers should use RunInTransaction instead.
func (db *DB) Begin() (*Tx, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	tx := &Tx{
		db:  db,
		_cn: cn,
	}
	if !noTx {
		if _, err := tx.Exec("BEGIN"); err != nil {
			tx.close()
			return nil, err
		}
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

func (tx *Tx) conn() *pool.Conn {
	tx._cn.SetReadTimeout(tx.db.opt.ReadTimeout)
	tx._cn.SetWriteTimeout(tx.db.opt.WriteTimeout)
	return tx._cn
}

// TODO(vmihailenco): track and close prepared statements
func (tx *Tx) Prepare(q string) (*Stmt, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()
	return prepare(tx.db, cn, q)
}

// Exec executes a query with the given parameters in a transaction.
func (tx *Tx) Exec(query interface{}, params ...interface{}) (types.Result, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()

	res, err := simpleQuery(cn, query, params...)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}
	return res, nil
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (tx *Tx) ExecOne(query interface{}, params ...interface{}) (types.Result, error) {
	res, err := tx.Exec(query, params...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res, nil)
}

// Query executes a query with the given parameters in a transaction.
func (tx *Tx) Query(model interface{}, query interface{}, params ...interface{}) (types.Result, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()
	res, err := simpleQueryData(cn, model, query, params...)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}
	return res, nil
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (tx *Tx) QueryOne(model interface{}, query interface{}, params ...interface{}) (types.Result, error) {
	mod, err := newSingleModel(model)
	if err != nil {
		return nil, err
	}
	res, err := tx.Query(mod, query, params...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res, mod)
}

// Commit commits the transaction.
func (tx *Tx) Commit() (err error) {
	if tx.done {
		return errTxDone
	}

	if !noTx {
		_, err = tx.Exec("COMMIT")
		if err != nil {
			tx.setErr(err)
		}
	}

	tx.close()
	return err
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() (err error) {
	if tx.done {
		return errTxDone
	}

	if !noTx {
		_, err = tx.Exec("ROLLBACK")
		if err != nil {
			tx.setErr(err)
		}
	}

	tx.close()
	return err
}

func (tx *Tx) setErr(e error) {
	tx.err = e
}

func (tx *Tx) close() error {
	if tx.done {
		return nil
	}
	tx.done = true
	return tx.db.freeConn(tx._cn, tx.err)
}

// CopyFrom copies data from reader to a table.
func (tx *Tx) CopyFrom(r io.Reader, query string, params ...interface{}) (types.Result, error) {
	cn := tx.conn()
	res, err := copyFrom(cn, r, query, params...)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}
	return res, nil
}
