package pg

import (
	"context"
	"errors"

	"github.com/go-pg/pg/v11/internal"
	"github.com/go-pg/pg/v11/internal/pool"
	"github.com/go-pg/pg/v11/orm"
	"github.com/go-pg/pg/v11/types"
)

var errStmtClosed = errors.New("pg: statement is closed")

// Stmt is a prepared statement. Stmt is safe for concurrent use by
// multiple goroutines.
type Stmt struct {
	db        *baseDB
	stickyErr error

	q       string
	name    string
	columns []types.ColumnInfo
}

func prepareStmt(ctx context.Context, db *baseDB, q string) (*Stmt, error) {
	stmt := &Stmt{
		db: db,

		q: q,
	}

	if err := stmt.prepare(ctx, q); err != nil {
		_ = stmt.Close(ctx)
		return nil, err
	}
	return stmt, nil
}

func (stmt *Stmt) prepare(ctx context.Context, q string) error {
	var lastErr error
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, stmt.db.retryBackoff(attempt-1)); err != nil {
				return err
			}

			err := stmt.db.pool.(*pool.StickyConnPool).Reset(ctx)
			if err != nil {
				return err
			}
		}

		lastErr = stmt.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
			var err error
			stmt.name, stmt.columns, err = stmt.db.prepare(ctx, cn, q)
			return err
		})
		if !stmt.db.shouldRetry(lastErr) {
			break
		}
	}
	return lastErr
}

func (stmt *Stmt) withConn(c context.Context, fn func(context.Context, *pool.Conn) error) error {
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
func (stmt *Stmt) Exec(ctx context.Context, params ...interface{}) (Result, error) {
	ctx, evt, err := stmt.db.beforeQuery(ctx, stmt.db.db, nil, stmt.q, params, nil)
	if err != nil {
		return nil, err
	}

	var res Result
	var lastErr error
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			lastErr = internal.Sleep(ctx, stmt.db.retryBackoff(attempt-1))
			if lastErr != nil {
				break
			}
		}

		lastErr = stmt.withConn(ctx, func(c context.Context, cn *pool.Conn) error {
			res, err = stmt.extQuery(ctx, cn, stmt.name, params...)
			return err
		})
		if !stmt.db.shouldRetry(lastErr) {
			break
		}
	}

	if err := stmt.db.afterQuery(ctx, evt, res, lastErr); err != nil {
		return nil, err
	}
	return res, lastErr
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (stmt *Stmt) ExecOne(ctx context.Context, params ...interface{}) (Result, error) {
	res, err := stmt.Exec(ctx, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Query executes a prepared query statement with the given parameters.
func (stmt *Stmt) Query(ctx context.Context, model interface{}, params ...interface{}) (Result, error) {
	ctx, evt, err := stmt.db.beforeQuery(ctx, stmt.db.db, model, stmt.q, params, nil)
	if err != nil {
		return nil, err
	}

	var res Result
	var lastErr error
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			lastErr = internal.Sleep(ctx, stmt.db.retryBackoff(attempt-1))
			if lastErr != nil {
				break
			}
		}

		lastErr = stmt.withConn(ctx, func(c context.Context, cn *pool.Conn) error {
			res, err = stmt.extQueryData(ctx, cn, stmt.name, model, stmt.columns, params...)
			return err
		})
		if !stmt.db.shouldRetry(lastErr) {
			break
		}
	}

	if err := stmt.db.afterQuery(ctx, evt, res, lastErr); err != nil {
		return nil, err
	}
	return res, lastErr
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (stmt *Stmt) QueryOne(ctx context.Context, model interface{}, params ...interface{}) (Result, error) {
	mod, err := orm.NewModel(model)
	if err != nil {
		return nil, err
	}

	res, err := stmt.Query(ctx, mod, params...)
	if err != nil {
		return nil, err
	}

	if err := internal.AssertOneRow(res.RowsAffected()); err != nil {
		return nil, err
	}
	return res, nil
}

// Close closes the statement.
func (stmt *Stmt) Close(ctx context.Context) error {
	var firstErr error

	if stmt.name != "" {
		firstErr = stmt.closeStmt(ctx)
	}

	if err := stmt.db.Close(ctx); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

func (stmt *Stmt) extQuery(
	ctx context.Context, cn *pool.Conn, name string, params ...interface{},
) (Result, error) {
	err := cn.WithWriter(ctx, stmt.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeBindExecuteMsg(wb, name, params...)
	})
	if err != nil {
		return nil, err
	}

	var res Result
	err = cn.WithReader(ctx, stmt.db.opt.ReadTimeout, func(rd *pool.ReaderContext) error {
		res, err = readExtQuery(rd)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (stmt *Stmt) extQueryData(
	ctx context.Context,
	cn *pool.Conn,
	name string,
	model interface{},
	columns []types.ColumnInfo,
	params ...interface{},
) (Result, error) {
	err := cn.WithWriter(ctx, stmt.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeBindExecuteMsg(wb, name, params...)
	})
	if err != nil {
		return nil, err
	}

	var res *result
	err = cn.WithReader(ctx, stmt.db.opt.ReadTimeout, func(rd *pool.ReaderContext) error {
		res, err = readExtQueryData(ctx, rd, model, columns)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (stmt *Stmt) closeStmt(ctx context.Context) error {
	return stmt.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		return stmt.db.closeStmt(ctx, cn, stmt.name)
	})
}
