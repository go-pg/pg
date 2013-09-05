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

func (stmt *Stmt) Query(f Fabric, args ...interface{}) ([]interface{}, error) {
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

func (stmt *Stmt) QueryOne(model interface{}, args ...interface{}) (interface{}, error) {
	res, err := stmt.Query(&fabricWrapper{model}, args...)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, ErrNoRows
	}
	if len(res) > 1 {
		return nil, ErrMultiRows
	}
	return res[0], nil
}

func (stmt *Stmt) Close() error {
	return stmt.pool.Put(stmt.cn)
}
