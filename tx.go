package pg

import (
	"github.com/golang/glog"
)

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

func (tx *Tx) Exec(q string, args ...interface{}) (*Result, error) {
	if tx.done {
		return nil, ErrTxDone
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
		return nil, ErrTxDone
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

func (tx *Tx) Commit() error {
	if tx.done {
		return ErrTxDone
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
		return ErrTxDone
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
