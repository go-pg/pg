package pg

import (
	"time"
)

// Not thread-safe.
type Stmt struct {
	db      *DB
	_cn     *conn
	name    string
	columns []string

	err error
}

func prepare(db *DB, cn *conn, q string) (*Stmt, error) {
	name := cn.GenId()
	writeParseDescribeSyncMsg(cn.buf, name, q)
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
		name:    name,
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
	backoff := defaultBackoff
	for i := 0; i < 3; i++ {
		var cn *conn

		cn, err = stmt.conn()
		if err != nil {
			return nil, err
		}

		res, err = extQuery(cn, stmt.name, args...)
		if !canRetry(err) {
			break
		}

		time.Sleep(backoff)
		backoff *= 2
	}
	if err != nil {
		stmt.setErr(err)
	}
	return
}

func (stmt *Stmt) ExecOne(args ...interface{}) (*Result, error) {
	res, err := stmt.Exec(args...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res)
}

func (stmt *Stmt) Query(f Factory, args ...interface{}) (res *Result, err error) {
	backoff := defaultBackoff
	for i := 0; i < 3; i++ {
		var cn *conn

		cn, err = stmt.conn()
		if err != nil {
			break
		}

		res, err = extQueryData(cn, stmt.name, f, stmt.columns, args...)
		if !canRetry(err) {
			break
		}

		time.Sleep(backoff)
		backoff *= 2
	}
	if err != nil {
		stmt.setErr(err)
	}
	return
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

func extQuery(cn *conn, name string, args ...interface{}) (*Result, error) {
	if err := writeBindExecuteMsg(cn.buf, name, args...); err != nil {
		return nil, err
	}
	if err := cn.Flush(); err != nil {
		return nil, err
	}
	return readExtQuery(cn)
}

func extQueryData(cn *conn, name string, f Factory, columns []string, args ...interface{}) (*Result, error) {
	if err := writeBindExecuteMsg(cn.buf, name, args...); err != nil {
		return nil, err
	}
	if err := cn.Flush(); err != nil {
		return nil, err
	}
	return readExtQueryData(cn, f, columns)
}
