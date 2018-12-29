package pg

import (
	"errors"
	"time"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/pool"
	"github.com/go-pg/pg/orm"
)

var errStmtClosed = errors.New("pg: statement is closed")

// Stmt is a prepared statement. Stmt is safe for concurrent use by
// multiple goroutines.
type Stmt struct {
	db        *baseDB
	stickyErr error

	q       string
	name    string
	columns [][]byte
}

func prepareStmt(db *baseDB, q string) (*Stmt, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	name, columns, err := db.prepare(cn, q)
	db.freeConn(cn, err)
	if err != nil {
		return nil, err
	}

	return &Stmt{
		db: db,

		q:       q,
		name:    name,
		columns: columns,
	}, nil
}

func (stmt *Stmt) conn() (*pool.Conn, error) {
	if stmt.stickyErr != nil {
		return nil, stmt.stickyErr
	}

	cn, err := stmt.db.conn()
	if err == pool.ErrClosed {
		return nil, errStmtClosed
	}
	return cn, err
}

func (stmt *Stmt) freeConn(cn *pool.Conn, err error) {
	stmt.db.freeConn(cn, err)
}

// Exec executes a prepared statement with the given parameters.
func (stmt *Stmt) Exec(params ...interface{}) (res orm.Result, err error) {
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt >= 1 {
			time.Sleep(stmt.db.retryBackoff(attempt - 1))
		}

		var cn *pool.Conn
		cn, err = stmt.conn()
		if err != nil {
			return nil, err
		}

		event := stmt.db.queryStarted(stmt.db.db, stmt.q, params, attempt)
		res, err = stmt.extQuery(cn, stmt.name, params...)
		stmt.db.queryProcessed(res, err, event)
		stmt.freeConn(cn, err)

		if !stmt.db.shouldRetry(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (stmt *Stmt) ExecOne(params ...interface{}) (orm.Result, error) {
	res, err := stmt.Exec(params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Query executes a prepared query statement with the given parameters.
func (stmt *Stmt) Query(model interface{}, params ...interface{}) (res orm.Result, err error) {
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt >= 1 {
			time.Sleep(stmt.db.retryBackoff(attempt - 1))
		}

		var cn *pool.Conn
		cn, err = stmt.conn()
		if err != nil {
			return nil, err
		}

		event := stmt.db.queryStarted(stmt.db.db, stmt.q, params, attempt)
		res, err = stmt.extQueryData(cn, stmt.name, model, stmt.columns, params...)
		stmt.db.queryProcessed(res, err, event)
		stmt.freeConn(cn, err)

		if !stmt.db.shouldRetry(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	if mod := res.Model(); mod != nil && res.RowsReturned() > 0 {
		if err = mod.AfterQuery(stmt.db.db); err != nil {
			return res, err
		}
	}

	return res, nil
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (stmt *Stmt) QueryOne(model interface{}, params ...interface{}) (orm.Result, error) {
	mod, err := orm.NewModel(model)
	if err != nil {
		return nil, err
	}

	res, err := stmt.Query(mod, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Close closes the statement.
func (stmt *Stmt) Close() error {
	err := stmt.closeStmt()
	if err != nil {
		return err
	}
	return stmt.db.Close()
}

func (stmt *Stmt) extQuery(cn *pool.Conn, name string, params ...interface{}) (orm.Result, error) {
	err := cn.WithWriter(stmt.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeBindExecuteMsg(wb, name, params...)
	})
	if err != nil {
		return nil, err
	}

	var res orm.Result
	err = cn.WithReader(stmt.db.opt.ReadTimeout, func(rd *internal.BufReader) error {
		res, err = readExtQuery(rd)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (stmt *Stmt) extQueryData(
	cn *pool.Conn, name string, model interface{}, columns [][]byte, params ...interface{},
) (orm.Result, error) {
	err := cn.WithWriter(stmt.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeBindExecuteMsg(wb, name, params...)
	})
	if err != nil {
		return nil, err
	}

	var res orm.Result
	err = cn.WithReader(stmt.db.opt.ReadTimeout, func(rd *internal.BufReader) error {
		res, err = readExtQueryData(rd, model, columns)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (stmt *Stmt) closeStmt() error {
	cn, err := stmt.conn()
	if err != nil {
		return err
	}

	err = stmt.db.closeStmt(cn, stmt.name)
	stmt.freeConn(cn, err)

	return err
}
