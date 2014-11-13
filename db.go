package pg

import (
	"io"
	"log"
	"net"
	"time"
)

const defaultBackoff = 100 * time.Millisecond

type Options struct {
	Network  string
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSL      bool

	// Params specify connection run-time configuration parameters.
	Params map[string]interface{}

	PoolSize int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	IdleTimeout        time.Duration
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
		return 5
	}
	return opt.PoolSize
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
	return opt.IdleCheckFrequency
}

func (opt *Options) getSSL() bool {
	if opt == nil {
		return false
	}
	return opt.SSL
}

func Connect(opt *Options) *DB {
	return &DB{
		opt:  opt,
		pool: newConnPool(opt),
	}
}

// Thread-safe.
type DB struct {
	opt  *Options
	pool *connPool
}

func (db *DB) Close() error {
	return db.pool.Close()
}

func (db *DB) conn() (*conn, error) {
	cn, isNew, err := db.pool.Get()
	if err != nil {
		return nil, err
	}

	if isNew {
		if err := setParams(cn, db.opt.Params); err != nil {
			return nil, err
		}
	}

	cn.SetReadTimeout(db.opt.ReadTimeout)
	cn.SetWriteTimeout(db.opt.WriteTimeout)
	return cn, nil
}

func (db *DB) freeConn(cn *conn, e error) error {
	if e == nil {
		return db.pool.Put(cn)
	}
	if cn.br.Buffered() > 0 {
		return db.pool.Remove(cn)
	}
	if pgerr, ok := e.(Error); ok && pgerr.Field('S') != "FATAL" {
		return db.pool.Put(cn)
	}
	if _, ok := e.(dbError); ok {
		return db.pool.Put(cn)
	}
	if neterr, ok := e.(net.Error); ok && neterr.Timeout() {
		if err := db.cancelRequest(cn.processId, cn.secretKey); err != nil {
			log.Printf("pg: cancelRequest failed: %s", err)
		}
	}
	return db.pool.Remove(cn)
}

func (db *DB) Prepare(q string) (*Stmt, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}
	return prepare(db, cn, q)
}

func (db *DB) Exec(q string, args ...interface{}) (res *Result, err error) {
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

func (db *DB) ExecOne(q string, args ...interface{}) (*Result, error) {
	res, err := db.Exec(q, args...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res)
}

func (db *DB) Query(f Factory, q string, args ...interface{}) (res *Result, err error) {
	backoff := defaultBackoff
	for i := 0; i < 3; i++ {
		var cn *conn

		cn, err = db.conn()
		if err != nil {
			break
		}

		res, err = simpleQueryData(cn, f, q, args...)
		db.freeConn(cn, err)
		if !canRetry(err) {
			break
		}

		time.Sleep(backoff)
		backoff *= 2
	}
	return
}

func (db *DB) QueryOne(model interface{}, q string, args ...interface{}) (*Result, error) {
	res, err := db.Query(&singleFactory{model}, q, args...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res)
}

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

func (db *DB) CopyFrom(r io.Reader, q string, args ...interface{}) (*Result, error) {
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

	if err := readCopyInResponse(cn); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	ready := make(chan struct{})
	var res *Result
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

		if err := cn.Flush(); err != nil {
			db.freeConn(cn, err)
			return nil, err
		}
	}

	select {
	case <-ready:
	default:
	}

	writeCopyDone(cn.buf)
	if err := cn.Flush(); err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	<-ready
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}

	db.pool.Put(cn)
	return res, nil
}

func (db *DB) CopyTo(w io.WriteCloser, q string, args ...interface{}) (*Result, error) {
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

func simpleQuery(cn *conn, q string, args ...interface{}) (*Result, error) {
	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		return nil, err
	}

	if err := cn.Flush(); err != nil {
		return nil, err
	}

	res, err := readSimpleQuery(cn)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func simpleQueryData(cn *conn, f Factory, q string, args ...interface{}) (*Result, error) {
	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		return nil, err
	}

	if err := cn.Flush(); err != nil {
		return nil, err
	}

	res, err := readSimpleQueryData(cn, f)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func assertOneAffected(res *Result) (*Result, error) {
	switch affected := res.Affected(); {
	case affected == 0:
		return nil, ErrNoRows
	case affected > 1:
		return nil, ErrMultiRows
	}
	return res, nil
}
