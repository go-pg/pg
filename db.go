package pg

import (
	"time"
)

type Options struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSL      bool

	PoolSize    int
	IdleTimeout time.Duration
}

func (opt *Options) getHost() string {
	if opt == nil || opt.Host == "" {
		return "localhost"
	}
	return opt.Host
}

func (opt *Options) getPort() string {
	if opt == nil || opt.Port == "" {
		return "5432"
	}
	return opt.Port
}

func (opt *Options) getUser() string {
	if opt == nil || opt.User == "" {
		return ""
	}
	return opt.User
}

func (opt *Options) getPassword() string {
	if opt == nil || opt.Password == "" {
		return ""
	}
	return opt.Password
}

func (opt *Options) getDatabase() string {
	if opt == nil || opt.Database == "" {
		return ""
	}
	return opt.Database
}

func (opt *Options) getPoolSize() int {
	if opt == nil || opt.PoolSize == 0 {
		return 5
	}
	return opt.PoolSize
}

func (opt *Options) getIdleTimeout() time.Duration {
	return opt.IdleTimeout
}

func (opt *Options) getSSL() bool {
	if opt == nil {
		return false
	}
	return opt.SSL
}

func Connect(opt *Options) *DB {
	dial := func() (*conn, error) {
		conn, err := dial(opt)
		if err != nil {
			return nil, err
		}
		if err := conn.Startup(); err != nil {
			return nil, err
		}
		return conn, nil
	}
	close := func(cn *conn) error {
		writeTerminateMsg(cn.buf)
		_ = cn.Flush()
		return cn.Close()
	}
	return &DB{
		pool: newDefaultPool(dial, close, opt.getPoolSize(), opt.getIdleTimeout()),
	}
}

type DB struct {
	pool *defaultPool
}

func (db *DB) Close() error {
	return db.pool.Close()
}

func (db *DB) conn() (*conn, error) {
	cn, _, err := db.pool.Get()
	if err != nil {
		return nil, err
	}
	return cn, nil
}

func (db *DB) freeConn(cn *conn, ei error) {
	if e, ok := ei.(Error); ok && e.GetField('S') != "FATAL" {
		db.pool.Put(cn)
	} else {
		db.pool.Remove(cn)
	}
}

func (db *DB) Prepare(q string) (*Stmt, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

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
		pool:    db.pool,
		cn:      cn,
		columns: columns,
	}
	return stmt, nil
}

func (db *DB) Exec(q string, args ...interface{}) (*Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		db.pool.Put(cn)
		return nil, err
	}

	if err := cn.Flush(); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	res, err := readSimpleQueryResult(cn)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	db.pool.Put(cn)
	return res, nil
}

func (db *DB) Query(f Fabric, q string, args ...interface{}) (*Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		db.pool.Put(cn)
		return nil, err
	}

	if err := cn.Flush(); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	res, err := readSimpleQueryData(cn, f)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	db.pool.Put(cn)
	return res, nil
}

func (db *DB) QueryOne(model interface{}, q string, args ...interface{}) (*Result, error) {
	res, err := db.Query(&fabricWrapper{model}, q, args...)
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

func (db *DB) Begin() (*Tx, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	tx := &Tx{
		pool: db.pool,
		cn:   cn,
	}
	if _, err := tx.Exec("BEGIN"); err != nil {
		tx.close()
		return nil, err
	}
	return tx, nil
}

func (db *DB) Listen(channels ...string) (*Listener, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	l := &Listener{
		pool: db.pool,
		cn:   cn,
	}
	if err := l.Listen(channels...); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}
