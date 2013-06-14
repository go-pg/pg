package pg

type Connector struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSL      bool

	PoolSize int
}

func (conn *Connector) getHost() string {
	if conn == nil || conn.Host == "" {
		return "localhost"
	}
	return conn.Host
}

func (conn *Connector) getPort() string {
	if conn == nil || conn.Port == "" {
		return "5432"
	}
	return conn.Port
}

func (conn *Connector) getUser() string {
	if conn == nil || conn.User == "" {
		return ""
	}
	return conn.User
}

func (conn *Connector) getPassword() string {
	if conn == nil || conn.Password == "" {
		return ""
	}
	return conn.Password
}

func (conn *Connector) getDatabase() string {
	if conn == nil || conn.Database == "" {
		return ""
	}
	return conn.Database
}

func (conn *Connector) getPoolSize() int {
	if conn == nil || conn.PoolSize == 0 {
		return 5
	}
	return conn.PoolSize
}

func (conn *Connector) getSSL() bool {
	if conn == nil {
		return false
	}
	return conn.SSL
}

func (connector *Connector) Connect() *DB {
	open := func() (interface{}, error) {
		conn, err := connect(connector)
		if err != nil {
			return nil, err
		}
		if err := conn.Startup(); err != nil {
			return nil, err
		}
		return conn, nil
	}
	close := func(i interface{}) error {
		conn := i.(*conn)
		return conn.Close()
	}
	return &DB{
		pool: newDefaultPool(open, close, connector.getPoolSize()),
	}
}

type DB struct {
	pool *defaultPool
}

func (db *DB) Close() error {
	return db.pool.Close()
}

func (db *DB) conn() (*conn, error) {
	i, _, err := db.pool.Get()
	if err != nil {
		return nil, err
	}
	return i.(*conn), nil
}

func (db *DB) freeConn(cn *conn, e error) {
	if _, ok := e.(Error); ok {
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

	if err := writeParseDescribeSyncMsg(cn, q); err != nil {
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

	if err := writeQueryMsg(cn, q, args...); err != nil {
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

func (db *DB) Query(f Fabric, q string, args ...interface{}) ([]interface{}, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	if err := writeQueryMsg(cn, q, args...); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	res, err := readSimpleQueryData(cn, f)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	db.pool.Put(cn)
	return res, err
}

func (db *DB) QueryOne(model interface{}, q string, args ...interface{}) (interface{}, error) {
	res, err := db.Query(&fabricWrapper{model}, q, args...)
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

func (db *DB) NewListener() (*Listener, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}
	return newListener(db.pool, cn), nil
}
