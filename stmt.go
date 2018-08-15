package pg

import (
	"errors"
	"sync"
	"time"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/pool"
	"github.com/go-pg/pg/orm"
)

var errStmtClosed = errors.New("pg: statement is closed")

// Stmt is a prepared statement. Stmt is safe for concurrent use by
// multiple goroutines.
type Stmt struct {
	db *DB

	mu   sync.Mutex
	_cn  *pool.Conn
	inTx bool

	q       string
	name    string
	columns [][]byte

	stickyErr error
}

// Prepare creates a prepared statement for later queries or
// executions. Multiple queries or executions may be run concurrently
// from the returned statement.
func (db *DB) Prepare(q string) (*Stmt, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	stmt, err := prepare(db, cn, q)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	return stmt, nil
}

func (stmt *Stmt) conn() (*pool.Conn, error) {
	if stmt._cn == nil {
		if stmt.stickyErr != nil {
			return nil, stmt.stickyErr
		}
		return nil, errStmtClosed
	}
	return stmt._cn, nil
}

func (stmt *Stmt) exec(params ...interface{}) (orm.Result, error) {
	stmt.mu.Lock()
	defer stmt.mu.Unlock()

	cn, err := stmt.conn()
	if err != nil {
		return nil, err
	}
	return stmt.extQuery(cn, stmt.name, params...)
}

// Exec executes a prepared statement with the given parameters.
func (stmt *Stmt) Exec(params ...interface{}) (res orm.Result, err error) {
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt >= 1 {
			time.Sleep(stmt.db.retryBackoff(attempt - 1))
		}

		start := time.Now()
		res, err = stmt.exec(params...)
		stmt.db.queryProcessed(stmt.db, start, stmt.q, params, attempt, res, err)

		if !stmt.db.shouldRetry(err) {
			break
		}
	}
	if err != nil {
		stmt.setErr(err)
	}
	return
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

func (stmt *Stmt) query(model interface{}, params ...interface{}) (orm.Result, error) {
	stmt.mu.Lock()
	defer stmt.mu.Unlock()

	cn, err := stmt.conn()
	if err != nil {
		return nil, err
	}

	res, err := stmt.extQueryData(cn, stmt.name, model, stmt.columns, params...)
	if err != nil {
		return nil, err
	}

	if mod := res.Model(); mod != nil && res.RowsReturned() > 0 {
		if err = mod.AfterQuery(stmt.db); err != nil {
			return res, err
		}
	}

	return res, nil
}

// Query executes a prepared query statement with the given parameters.
func (stmt *Stmt) Query(model interface{}, params ...interface{}) (res orm.Result, err error) {
	for attempt := 0; attempt <= stmt.db.opt.MaxRetries; attempt++ {
		if attempt >= 1 {
			time.Sleep(stmt.db.retryBackoff(attempt - 1))
		}

		start := time.Now()
		res, err = stmt.query(model, params...)
		stmt.db.queryProcessed(stmt.db, start, stmt.q, params, attempt, res, err)

		if !stmt.db.shouldRetry(err) {
			break
		}
	}
	if err != nil {
		stmt.setErr(err)
	}
	return
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

func (stmt *Stmt) setErr(e error) {
	if stmt.stickyErr == nil {
		stmt.stickyErr = e
	}
}

// Close closes the statement.
func (stmt *Stmt) Close() error {
	stmt.mu.Lock()
	defer stmt.mu.Unlock()

	if stmt._cn == nil {
		return errStmtClosed
	}

	err := stmt.closeStmt(stmt._cn, stmt.name)
	if !stmt.inTx {
		stmt.db.freeConn(stmt._cn, err)
	}
	stmt._cn = nil
	return err
}

func prepare(db *DB, cn *pool.Conn, q string) (*Stmt, error) {
	name := cn.NextId()
	err := cn.WithWriter(db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeParseDescribeSyncMsg(wb, name, q)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var columns [][]byte
	cn.WithReader(db.opt.ReadTimeout, func(rd *pool.Reader) error {
		columns, err = readParseDescribeSync(rd)
		return err
	})
	if err != nil {
		return nil, err
	}

	stmt := &Stmt{
		db:      db,
		_cn:     cn,
		q:       q,
		name:    name,
		columns: columns,
	}
	return stmt, nil
}

func (stmt *Stmt) extQuery(cn *pool.Conn, name string, params ...interface{}) (orm.Result, error) {
	err := cn.WithWriter(stmt.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		return writeBindExecuteMsg(wb, name, params...)
	})
	if err != nil {
		return nil, err
	}

	var res orm.Result
	err = cn.WithReader(stmt.db.opt.ReadTimeout, func(rd *pool.Reader) error {
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
	err = cn.WithReader(stmt.db.opt.ReadTimeout, func(rd *pool.Reader) error {
		res, err = readExtQueryData(rd, model, columns)
		return err
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (stmt *Stmt) closeStmt(cn *pool.Conn, name string) error {
	err := cn.WithWriter(stmt.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeCloseMsg(wb, name)
		writeFlushMsg(wb)
		return nil
	})
	if err != nil {
		return err
	}

	err = cn.WithReader(stmt.db.opt.ReadTimeout, func(rd *pool.Reader) error {
		return readCloseCompleteMsg(rd)
	})
	return err
}
