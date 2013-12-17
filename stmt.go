package pg

// Not thread-safe.
type Stmt struct {
	db      *DB
	_cn     *conn
	columns []string

	err error
}

func prepare(db *DB, cn *conn, q string) (*Stmt, error) {
	writeParseDescribeSyncMsg(cn.buf, q)
	if err := cn.Flush(); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	columns, err := readParseDescribeSync(cn)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	stmt := &Stmt{
		db:      db,
		_cn:     cn,
		columns: columns,
	}
	return stmt, nil
}

func (stmt *Stmt) conn() (*conn, error) {
	if stmt._cn == nil {
		return nil, errStmtClosed
	}
	stmt._cn.SetReadTimeout(stmt.db.opt.ReadTimeout)
	stmt._cn.SetWriteTimeout(stmt.db.opt.WriteTimeout)
	return stmt._cn, nil
}

func (stmt *Stmt) Exec(args ...interface{}) (*Result, error) {
	cn, err := stmt.conn()
	if err != nil {
		return nil, err
	}

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

func (stmt *Stmt) ExecOne(args ...interface{}) (*Result, error) {
	res, err := stmt.Exec(args...)
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

func (stmt *Stmt) Query(f Factory, args ...interface{}) (*Result, error) {
	cn, err := stmt.conn()
	if err != nil {
		return nil, err
	}

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
	if stmt._cn == nil {
		return errStmtClosed
	}
	err := stmt.db.freeConn(stmt._cn, stmt.err)
	stmt._cn = nil
	return err
}
