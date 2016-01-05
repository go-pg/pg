package pg

import (
	"io"
	"os"
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
	_cn *conn

	err  error
	done bool
}

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

func (tx *Tx) conn() *conn {
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

func (tx *Tx) Exec(q string, args ...interface{}) (Result, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()

	res, err := simpleQuery(cn, q, args...)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}
	return res, nil
}

func (tx *Tx) ExecOne(q string, args ...interface{}) (Result, error) {
	res, err := tx.Exec(q, args...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res, nil)
}

func (tx *Tx) Query(coll interface{}, q string, args ...interface{}) (Result, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()
	res, err := simpleQueryData(cn, coll, q, args...)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}
	return res, nil
}

func (tx *Tx) QueryOne(record interface{}, q string, args ...interface{}) (Result, error) {
	coll := &singleRecordCollection{record: record}
	res, err := tx.Query(coll, q, args...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res, coll)
}

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
func (tx *Tx) CopyFrom(r io.Reader, q string, args ...interface{}) (Result, error) {
	cn := tx.conn()
	res, err := copyFrom(cn, r, q, args...)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}
	return res, nil
}
