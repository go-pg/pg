package pg

type Tx struct {
	pool     *defaultPool
	cn       *conn
	cnBroken bool

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

func (tx *Tx) Query(f Fabric, q string, args ...interface{}) (*Result, error) {
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

func (tx *Tx) setErr(ei error) {
	if e, ok := ei.(Error); !ok || e.GetField('S') == "FATAL" {
		tx.cnBroken = true
	}
}

func (tx *Tx) close() error {
	tx.done = true
	if tx.cnBroken {
		return tx.pool.Remove(tx.cn)
	} else {
		return tx.pool.Put(tx.cn)
	}
}
