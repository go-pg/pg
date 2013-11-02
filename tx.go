package pg

import (
	"github.com/golang/glog"
)

type Tx struct {
	db *DB
	cn *conn

	err  error
	done bool
}

func (tx *Tx) Exec(q string, args ...interface{}) (*Result, error) {
	if tx.done {
		return nil, ErrTxDone
	}

	if err := writeQueryMsg(tx.cn.buf, q, args...); err != nil {
		return nil, err
	}

	if err := tx.cn.Flush(); err != nil {
		tx.setErr(err)
		return nil, err
	}

	res, err := readSimpleQueryResult(tx.cn)
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

	if err := writeQueryMsg(tx.cn.buf, q, args...); err != nil {
		return nil, err
	}

	if err := tx.cn.Flush(); err != nil {
		tx.setErr(err)
		return nil, err
	}

	res, err := readSimpleQueryData(tx.cn, f)
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
	return tx.db.freeConn(tx.cn, tx.err)
}

func txFinalizer(tx *Tx) {
	if !tx.done {
		glog.Errorf("transaction was neither commited or rollbacked")
	}
}
