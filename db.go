package pg

import (
	"io"
	"log"
	"net"
	"time"
)

const defaultBackoff = 100 * time.Millisecond

// Database connection options.
type Options struct {
	// The network type, either tcp or unix.
	// Default is tcp.
	Network  string
	Host     string
	Port     string
	User     string
	Password string
	Database string
	// Whether to use secure TCP/IP connections (TLS).
	SSL bool

	// Run-time configuration parameters to be set on connection.
	Params map[string]interface{}

	// The deadline for establishing new connections. If reached,
	// dial will fail with a timeout.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// The timeout for socket reads. If reached, commands will fail
	// with a timeout error instead of blocking.
	// Default is no timeout.
	ReadTimeout time.Duration
	// The timeout for socket writes. If reached, commands will fail
	// with a timeout error instead of blocking.
	// Default is no timeout.
	WriteTimeout time.Duration

	// The maximum number of open socket connections.
	// Default is 10 connections.
	PoolSize int
	// The amount of time client waits for free connection if all
	// connections are busy before returning an error.
	// Default is 5 seconds.
	PoolTimeout time.Duration
	// The amount of time after which client closes idle connections.
	// Default is to not close idle connections.
	IdleTimeout time.Duration
	// The frequency of idle checks.
	// Default is 1 minute.
	IdleCheckFrequency time.Duration
}

func (opt *Options) getNetwork() string {
	if opt == nil || opt.Network == "" {
		return "tcp"
	}
	return opt.Network
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

func (opt *Options) getAddr() string {
	switch opt.getNetwork() {
	case "tcp":
		return net.JoinHostPort(opt.getHost(), opt.getPort())
	default:
		return opt.getHost()
	}
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
		return 10
	}
	return opt.PoolSize
}

func (opt *Options) getPoolTimeout() time.Duration {
	if opt == nil || opt.PoolTimeout == 0 {
		return 5 * time.Second
	}
	return opt.PoolTimeout
}

func (opt *Options) getDialTimeout() time.Duration {
	if opt.DialTimeout == 0 {
		return 5 * time.Second
	}
	return opt.DialTimeout
}

func (opt *Options) getIdleTimeout() time.Duration {
	return opt.IdleTimeout
}

func (opt *Options) getIdleCheckFrequency() time.Duration {
	if opt.IdleCheckFrequency == 0 {
		return time.Minute
	}
	return opt.IdleCheckFrequency
}

func (opt *Options) getSSL() bool {
	if opt == nil {
		return false
	}
	return opt.SSL
}

// Connect connects to a database using provided options.
//
// The returned DB is safe for concurrent use by multiple goroutines
// and maintains its own connection pool.
func Connect(opt *Options) *DB {
	return &DB{
		opt:  opt,
		pool: newConnPool(opt),
	}
}

// DB is a database handle representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type DB struct {
	opt  *Options
	pool *connPool
}

// Options returns read-only Options that were used to connect to the DB.
func (db *DB) Options() *Options {
	return db.opt
}

// WithTimeout returns a DB that uses d as the read/write timeout.
func (db *DB) WithTimeout(d time.Duration) *DB {
	newopt := *db.opt
	newopt.ReadTimeout = d
	newopt.WriteTimeout = d
	return &DB{
		opt:  &newopt,
		pool: db.pool,
	}
}

func (db *DB) conn() (*conn, error) {
	cn, _, err := db.pool.Get()
	if err != nil {
		return nil, err
	}

	cn.SetReadTimeout(db.opt.ReadTimeout)
	cn.SetWriteTimeout(db.opt.WriteTimeout)
	return cn, nil
}

func (db *DB) freeConn(cn *conn, err error) error {
	if err == nil {
		return db.pool.Put(cn)
	}
	if cn.rd.Buffered() > 0 {
		return db.pool.Remove(cn, err)
	}
	if pgerr, ok := err.(Error); ok && pgerr.Field('S') != "FATAL" {
		return db.pool.Put(cn)
	}
	if _, ok := err.(dbError); ok {
		return db.pool.Put(cn)
	}
	if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
		if err := db.cancelRequest(cn.processId, cn.secretKey); err != nil {
			log.Printf("pg: cancelRequest failed: %s", err)
		}
	}
	return db.pool.Remove(cn, err)
}

// Close closes the database client, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (db *DB) Close() error {
	return db.pool.Close()
}

// Exec executes a query ignoring returned rows. The args are for
// any placeholder parameters in the query.
func (db *DB) Exec(q string, args ...interface{}) (res Result, err error) {
	backoff := defaultBackoff
	for i := 0; i < 3; i++ {
		var cn *conn

		cn, err = db.conn()
		if err != nil {
			return nil, err
		}

		res, err = simpleQuery(cn, q, args...)
		db.freeConn(cn, err)
		if !canRetry(err) {
			break
		}

		time.Sleep(backoff)
		backoff *= 2
	}
	return
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *DB) ExecOne(q string, args ...interface{}) (Result, error) {
	res, err := db.Exec(q, args...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res, nil)
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func (db *DB) Query(coll interface{}, q string, args ...interface{}) (res Result, err error) {
	backoff := defaultBackoff
	for i := 0; i < 3; i++ {
		var cn *conn

		cn, err = db.conn()
		if err != nil {
			break
		}

		res, err = simpleQueryData(cn, coll, q, args...)
		db.freeConn(cn, err)
		if !canRetry(err) {
			break
		}

		time.Sleep(backoff)
		backoff *= 2
	}
	return
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *DB) QueryOne(record interface{}, q string, args ...interface{}) (Result, error) {
	coll := &singleRecordCollection{record: record}
	res, err := db.Query(coll, q, args...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res, coll)
}

// Listen listens for notifications sent by NOTIFY statement.
func (db *DB) Listen(channels ...string) (*Listener, error) {
	l := &Listener{
		db: db,
	}
	if err := l.Listen(channels...); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

// CopyFrom copies data from reader to a table.
func (db *DB) CopyFrom(r io.Reader, q string, args ...interface{}) (Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}
	res, err := copyFrom(cn, r, q, args...)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}
	db.pool.Put(cn)
	return res, nil
}

// CopyTo copies data from a table to writer.
func (db *DB) CopyTo(w io.WriteCloser, q string, args ...interface{}) (Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		db.pool.Put(cn)
		return nil, err
	}

	if err := cn.FlushWrite(); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	if err := readCopyOutResponse(cn); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	res, err := readCopyData(cn, w)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	return res, nil
}

func setParams(cn *conn, params map[string]interface{}) error {
	for key, value := range params {
		_, err := simpleQuery(cn, "SET ? = ?", F(key), value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) cancelRequest(processId, secretKey int32) error {
	cn, err := dial(db.opt)
	if err != nil {
		return err
	}

	buf := newBuffer()
	writeCancelRequestMsg(buf, processId, secretKey)
	_, err = cn.Write(buf.Flush())
	if err != nil {
		return err
	}

	return cn.Close()
}

func simpleQuery(cn *conn, q string, args ...interface{}) (Result, error) {
	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		return nil, err
	}

	if err := cn.FlushWrite(); err != nil {
		return nil, err
	}

	res, err := readSimpleQuery(cn)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func simpleQueryData(cn *conn, coll interface{}, q string, args ...interface{}) (Result, error) {
	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		return nil, err
	}

	if err := cn.FlushWrite(); err != nil {
		return nil, err
	}

	res, err := readSimpleQueryData(cn, coll)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func assertOne(l int) error {
	switch {
	case l == 0:
		return ErrNoRows
	case l > 1:
		return ErrMultiRows
	default:
		return nil
	}
}

func assertOneAffected(res Result, coll *singleRecordCollection) (Result, error) {
	if err := assertOne(res.Affected()); err != nil {
		return nil, err
	}
	if coll != nil {
		if err := assertOne(coll.Len()); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func copyFrom(cn *conn, r io.Reader, q string, args ...interface{}) (Result, error) {
	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		return nil, err
	}

	if err := cn.FlushWrite(); err != nil {
		return nil, err
	}

	if err := readCopyInResponse(cn); err != nil {
		return nil, err
	}

	ready := make(chan struct{})
	var res Result
	var err error
	go func() {
		res, err = readReadyForQuery(cn)
		close(ready)
	}()

	for {
		select {
		case <-ready:
			break
		default:
		}

		_, err := writeCopyData(cn.buf, r)
		if err == io.EOF {
			break
		}

		if err := cn.FlushWrite(); err != nil {
			return nil, err
		}
	}

	select {
	case <-ready:
	default:
	}

	writeCopyDone(cn.buf)
	if err := cn.FlushWrite(); err != nil {
		return nil, err
	}

	<-ready
	if err != nil {
		return nil, err
	}
	return res, nil
}
