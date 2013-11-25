package pg

import (
	"github.com/golang/glog"
)

// Not thread-safe.
type Tx struct {
	db  *DB
	_cn *conn

	err  error
	done bool
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

func (tx *Tx) Exec(q string, args ...interface{}) (*Result, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()

	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		return nil, err
	}

	if err := cn.Flush(); err != nil {
		tx.setErr(err)
		return nil, err
	}

	res, err := readSimpleQueryResult(cn)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}

	return res, nil
}

func (tx *Tx) Query(f Factory, q string, args ...interface{}) (*Result, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()

	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		return nil, err
	}

	if err := cn.Flush(); err != nil {
		tx.setErr(err)
		return nil, err
	}

	res, err := readSimpleQueryData(cn, f)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}

	return res, nil
}

func (tx *Tx) QueryOne(model interface{}, q string, args ...interface{}) (*Result, error) {
	res, err := tx.Query(&singleFactory{model}, q, args...)
	if err != nil {
		return nil, err
	}

	switch affected := res.Affected(); {
	case affected == 0:
		return nil, ErrNoRows
	case affected > 1:
		return nil, ErrMultiRows
	}

	return res, nil
}

func (tx *Tx) Commit() error {
	if tx.done {
		return errTxDone
	}
	_, err := tx.Exec("COMMIT")
	if err != nil {
		tx.setErr(err)
	}
	tx.close()
	return err
}

func (tx *Tx) Rollback() error {
	if tx.done {
		return errTxDone
	}
	_, err := tx.Exec("ROLLBACK")
	if err != nil {
		tx.setErr(err)
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

func txFinalizer(tx *Tx) {
	if !tx.done {
		glog.Errorf("transaction was neither commited or rollbacked")
	}
}
