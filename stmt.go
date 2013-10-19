package pg

type Stmt struct {
	pool    *defaultPool
	cn      *conn
	columns []string
}

func (stmt *Stmt) Exec(args ...interface{}) (*Result, error) {
	if err := writeBindExecuteMsg(stmt.cn.buf, args...); err != nil {
		return nil, err
	}

	if err := stmt.cn.Flush(); err != nil {
		return nil, err
	}

	res, err := readExtQueryResult(stmt.cn)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (stmt *Stmt) Query(f Factory, args ...interface{}) (*Result, error) {
	if err := writeBindExecuteMsg(stmt.cn.buf, args...); err != nil {
		return nil, err
	}

	if err := stmt.cn.Flush(); err != nil {
		return nil, err
	}

	res, err := readExtQueryData(stmt.cn, f, stmt.columns)
	if err != nil {
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

func (stmt *Stmt) Close() error {
	return stmt.pool.Put(stmt.cn)
}
