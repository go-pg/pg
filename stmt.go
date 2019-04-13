package pg

import (
	"context"
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
	stmt := &Stmt{
		db: db,

		q: q,
	}

	err := stmt.prepare(q)
	if err != nil {
		_ = stmt.Close()
		return nil, err
	}
	return stmt, nil
}

func (stmt *Stmt) prepare(q string) error {
	var lastErr error
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(stmt.db.retryBackoff(attempt - 1))

			err := stmt.db.pool.(*pool.SingleConnPool).Reset()
			if err != nil {
				internal.Logf(err.Error())
				continue
			}
		}

		lastErr = stmt.withConn(nil, func(cn *pool.Conn) error {
			var err error
			stmt.name, stmt.columns, err = stmt.db.prepare(cn, q)
			return err
		})
		if !stmt.db.shouldRetry(lastErr) {
			break
		}
	}
	return lastErr
}

func (stmt *Stmt) withConn(c context.Context, fn func(cn *pool.Conn) error) error {
	if stmt.stickyErr != nil {
		return stmt.stickyErr
	}
	err := stmt.db.withConn(c, fn)
	if err == pool.ErrClosed {
		return errStmtClosed
	}
	return err
}

// Exec executes a prepared statement with the given parameters.
func (stmt *Stmt) Exec(params ...interface{}) (Result, error) {
	return stmt.exec(nil, params...)
}

// ExecContext executes a prepared statement with the given parameters.
func (stmt *Stmt) ExecContext(c context.Context, params ...interface{}) (Result, error) {
	return stmt.exec(c, params...)
}

func (stmt *Stmt) exec(c context.Context, params ...interface{}) (res Result, err error) {
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(stmt.db.retryBackoff(attempt - 1))
		}

		err = stmt.withConn(c, func(cn *pool.Conn) error {
			event := stmt.db.queryStarted(c, stmt.db.db, stmt.q, params, attempt)
			res, err = stmt.extQuery(cn, stmt.name, params...)
			stmt.db.queryProcessed(res, err, event)
			return err
		})
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
func (stmt *Stmt) ExecOne(params ...interface{}) (Result, error) {
	return stmt.execOne(nil, params...)
}

func (stmt *Stmt) ExecOneContext(c context.Context, params ...interface{}) (Result, error) {
	return stmt.execOne(c, params...)
}

func (stmt *Stmt) execOne(c context.Context, params ...interface{}) (Result, error) {
	res, err := stmt.ExecContext(c, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Query executes a prepared query statement with the given parameters.
func (stmt *Stmt) Query(model interface{}, params ...interface{}) (Result, error) {
	return stmt.query(nil, model, params...)
}

func (stmt *Stmt) QueryContext(c context.Context, model interface{}, params ...interface{}) (Result, error) {
	return stmt.query(c, model, params...)
}

func (stmt *Stmt) query(c context.Context, model interface{}, params ...interface{}) (res Result, err error) {
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(stmt.db.retryBackoff(attempt - 1))
		}

		err = stmt.withConn(c, func(cn *pool.Conn) error {
			event := stmt.db.queryStarted(c, stmt.db.db, stmt.q, params, attempt)
			res, err = stmt.extQueryData(cn, stmt.name, model, stmt.columns, params...)
			stmt.db.queryProcessed(res, err, event)
			return err
		})
		if !stmt.db.shouldRetry(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	if mod := res.Model(); mod != nil && res.RowsReturned() > 0 {
		if err = mod.AfterQuery(c, stmt.db.db); err != nil {
			return res, err
		}
	}

	return res, nil
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (stmt *Stmt) QueryOne(model interface{}, params ...interface{}) (Result, error) {
	return stmt.queryOne(nil, model, params...)
}

func (stmt *Stmt) QueryOneContext(c context.Context, model interface{}, params ...interface{}) (Result, error) {
	return stmt.queryOne(c, model, params...)
}

func (stmt *Stmt) queryOne(c context.Context, model interface{}, params ...interface{}) (Result, error) {
	mod, err := orm.NewModel(model)
	if err != nil {
		return nil, err
	}

	res, err := stmt.QueryContext(c, mod, params...)
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
	var firstErr error

	if stmt.name != "" {
		firstErr = stmt.closeStmt()
	}

	err := stmt.db.Close()
	if err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

func (stmt *Stmt) extQuery(cn *pool.Conn, name string, params ...interface{}) (Result, error) {
	err := cn.WithWriter(stmt.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeBindExecuteMsg(wb, name, params...)
	})
	if err != nil {
		return nil, err
	}

	var res Result
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
) (Result, error) {
	err := cn.WithWriter(stmt.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeBindExecuteMsg(wb, name, params...)
	})
	if err != nil {
		return nil, err
	}

	var res Result
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
	return stmt.withConn(nil, func(cn *pool.Conn) error {
		return stmt.db.closeStmt(cn, stmt.name)
	})
}
