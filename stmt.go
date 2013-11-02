package pg

type Stmt struct {
	db      *DB
	_cn     *conn
	columns []string

	err    error
	closed bool
}

func (stmt *Stmt) conn() *conn {
	stmt._cn.SetReadTimeout(stmt.db.opt.ReadTimeout)
	stmt._cn.SetWriteTimeout(stmt.db.opt.WriteTimeout)
	return stmt._cn
}

func (stmt *Stmt) Exec(args ...interface{}) (*Result, error) {
	if stmt.closed {
		return nil, errStmtClosed
	}

	cn := stmt.conn()

	if err := writeBindExecuteMsg(cn.buf, args...); err != nil {
		return nil, err
	}

	if err := cn.Flush(); err != nil {
		stmt.setErr(err)
		return nil, err
	}

	res, err := readExtQueryResult(cn)
	if err != nil {
		stmt.setErr(err)
		return nil, err
	}

	return res, nil
}

func (stmt *Stmt) Query(f Factory, args ...interface{}) (*Result, error) {
	if stmt.closed {
		return nil, errStmtClosed
	}

	cn := stmt.conn()

	if err := writeBindExecuteMsg(cn.buf, args...); err != nil {
		return nil, err
	}

	if err := cn.Flush(); err != nil {
		stmt.setErr(err)
		return nil, err
	}

	res, err := readExtQueryData(cn, f, stmt.columns)
	if err != nil {
		stmt.setErr(err)
		return nil, err
	}

	return res, err
}

func (stmt *Stmt) QueryOne(model interface{}, args ...interface{}) (*Result, error) {
	res, err := stmt.Query(&singleFactory{model}, args...)
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

func (stmt *Stmt) setErr(e error) {
	stmt.err = e
}

func (stmt *Stmt) Close() error {
	if stmt.closed {
		return nil
	}
	stmt.closed = true
	return stmt.db.freeConn(stmt._cn, stmt.err)
}
