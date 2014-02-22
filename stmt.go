package pg

import (
	"time"
)

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

func (stmt *Stmt) Exec(args ...interface{}) (res *Result, err error) {
	cn, err := stmt.conn()
	if err != nil {
		return nil, err
	}

	backoff := 100 * time.Millisecond
	for i := 0; i < 3; i++ {
		res, err = extQuery(cn, args...)
		if err != nil {
			if pgerr, ok := err.(*pgError); ok && pgerr.Field('C') == "40001" {
				time.Sleep(backoff)
				backoff *= 2
				continue
			}
		}
		break
	}

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
	return assertOneAffected(res)
}

func (stmt *Stmt) Query(f Factory, args ...interface{}) (res *Result, err error) {
	cn, err := stmt.conn()
	if err != nil {
		return nil, err
	}

	backoff := 100 * time.Millisecond
	for i := 0; i < 3; i++ {
		res, err = extQueryData(cn, f, stmt.columns, args...)
		if err != nil {
			if pgerr, ok := err.(*pgError); ok && pgerr.Field('C') == "40001" {
				time.Sleep(backoff)
				backoff *= 2
				continue
			}
		}
		break
	}

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
	return assertOneAffected(res)
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

func extQuery(cn *conn, args ...interface{}) (*Result, error) {
	if err := writeBindExecuteMsg(cn.buf, args...); err != nil {
		return nil, err
	}
	if err := cn.Flush(); err != nil {
		return nil, err
	}
	return readExtQuery(cn)
}

func extQueryData(cn *conn, f Factory, columns []string, args ...interface{}) (*Result, error) {
	if err := writeBindExecuteMsg(cn.buf, args...); err != nil {
		return nil, err
	}
	if err := cn.Flush(); err != nil {
		return nil, err
	}
	return readExtQueryData(cn, f, columns)
}
