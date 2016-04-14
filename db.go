package pg // import "gopkg.in/pg.v4"

import (
	"io"
	"time"

	"gopkg.in/pg.v4/internal/pool"
	"gopkg.in/pg.v4/orm"
	"gopkg.in/pg.v4/types"
)

const defaultBackoff = 500 * time.Millisecond

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
	pool *pool.ConnPool
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

func (db *DB) conn() (*pool.Conn, error) {
	cn, err := db.pool.Get()
	if err != nil {
		return nil, err
	}

	if !cn.Inited {
		if err := db.initConn(cn); err != nil {
			_ = db.pool.Remove(cn, err)
			return nil, err
		}
	}

	cn.SetReadTimeout(db.opt.ReadTimeout)
	cn.SetWriteTimeout(db.opt.WriteTimeout)
	return cn, nil
}

func (db *DB) initConn(cn *pool.Conn) error {
	if db.opt.getSSL() {
		if err := enableSSL(cn); err != nil {
			return err
		}
	}

	err := startup(cn, db.opt.getUser(), db.opt.getPassword(), db.opt.getDatabase())
	if err != nil {
		return err
	}

	if err := setParams(cn, db.opt.Params); err != nil {
		return err
	}

	cn.Inited = true
	return nil
}

func (db *DB) freeConn(cn *pool.Conn, err error) error {
	if !isBadConn(err, false) {
		return db.pool.Put(cn)
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

// Exec executes a query ignoring returned rows. The params are for
// any placeholder parameters in the query.
func (db *DB) Exec(query interface{}, params ...interface{}) (res *types.Result, err error) {
	for i := 0; ; i++ {
		var cn *pool.Conn

		cn, err = db.conn()
		if err != nil {
			return nil, err
		}

		res, err = simpleQuery(cn, query, params...)
		db.freeConn(cn, err)

		if i >= db.opt.MaxRetries {
			return res, err
		}
		if !shouldRetry(err) {
			return res, err
		}

		time.Sleep(defaultBackoff << uint(i))
	}
	return
}

// ExecOne acts like Exec, but query must affect only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *DB) ExecOne(query interface{}, params ...interface{}) (*types.Result, error) {
	res, err := db.Exec(query, params...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res)
}

// Query executes a query that returns rows, typically a SELECT.
// The params are for any placeholder parameters in the query.
func (db *DB) Query(model, query interface{}, params ...interface{}) (res *types.Result, err error) {
	for i := 0; i < 3; i++ {
		var cn *pool.Conn

		cn, err = db.conn()
		if err != nil {
			return nil, err
		}

		res, err = simpleQueryData(cn, model, query, params...)
		db.freeConn(cn, err)

		if i >= db.opt.MaxRetries {
			return res, err
		}
		if !shouldRetry(err) {
			return res, err
		}

		time.Sleep(defaultBackoff << uint(i))
	}
	return
}

// QueryOne acts like Query, but query must return only one row. It
// returns ErrNoRows error when query returns zero rows or
// ErrMultiRows when query returns multiple rows.
func (db *DB) QueryOne(model, query interface{}, params ...interface{}) (*types.Result, error) {
	mod, err := newSingleModel(model)
	if err != nil {
		return nil, err
	}
	res, err := db.Query(mod, query, params...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res)
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
func (db *DB) CopyFrom(r io.Reader, query interface{}, params ...interface{}) (*types.Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}
	res, err := copyFrom(cn, r, query, params...)
	if err != nil {
		db.freeConn(cn, err)
		return nil, err
	}
	db.pool.Put(cn)
	return res, nil
}

// CopyTo copies data from a table to writer.
func (db *DB) CopyTo(w io.Writer, query interface{}, params ...interface{}) (*types.Result, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	if err := writeQueryMsg(cn.Wr, query, params...); err != nil {
		db.pool.Put(cn)
		return nil, err
	}

	if err := cn.Wr.Flush(); err != nil {
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

// Model returns new query for the model.
func (db *DB) Model(model interface{}) *orm.Query {
	return orm.NewQuery(db, model)
}

// Create inserts the model into database.
func (db *DB) Create(model interface{}) error {
	return orm.Create(db, model)
}

// Update updates the model in database.
func (db *DB) Update(model interface{}) error {
	return orm.Update(db, model)
}

// Delete deletes the model from database.
func (db *DB) Delete(model interface{}) error {
	return orm.Delete(db, model)
}

func (db *DB) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return orm.Formatter{}.Append(dst, query, params...)
}

func setParams(cn *pool.Conn, params map[string]interface{}) error {
	for key, value := range params {
		_, err := simpleQuery(cn, "SET ? = ?", F(key), value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) cancelRequest(processId, secretKey int32) error {
	cn, err := db.pool.NewConn()
	if err != nil {
		return err
	}

	writeCancelRequestMsg(cn.Wr, processId, secretKey)
	if err = cn.Wr.Flush(); err != nil {
		return err
	}
	cn.Close()

	return nil
}

func simpleQuery(cn *pool.Conn, query interface{}, params ...interface{}) (*types.Result, error) {
	if err := writeQueryMsg(cn.Wr, query, params...); err != nil {
		return nil, err
	}

	if err := cn.Wr.Flush(); err != nil {
		return nil, err
	}

	res, err := readSimpleQuery(cn)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func simpleQueryData(cn *pool.Conn, model, query interface{}, params ...interface{}) (*types.Result, error) {
	if err := writeQueryMsg(cn.Wr, query, params...); err != nil {
		return nil, err
	}

	if err := cn.Wr.Flush(); err != nil {
		return nil, err
	}

	res, err := readSimpleQueryData(cn, model)
	if err != nil {
		return nil, err
	}

	return res, nil
}

type singleModel struct {
	orm.Model
}

var _ orm.Collection = (*singleModel)(nil)

func newSingleModel(mod interface{}) (*singleModel, error) {
	model, ok := mod.(orm.Model)
	if !ok {
		var err error
		model, err = orm.NewModel(mod)
		if err != nil {
			return nil, err
		}
	}
	return &singleModel{
		Model: model,
	}, nil
}

func (m *singleModel) AddModel(_ orm.ColumnScanner) error {
	return nil
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

func assertOneAffected(res *types.Result) (*types.Result, error) {
	if err := assertOne(res.Affected()); err != nil {
		return nil, err
	}
	return res, nil
}

func copyFrom(cn *pool.Conn, r io.Reader, query interface{}, params ...interface{}) (*types.Result, error) {
	if err := writeQueryMsg(cn.Wr, query, params...); err != nil {
		return nil, err
	}

	if err := cn.Wr.Flush(); err != nil {
		return nil, err
	}

	if err := readCopyInResponse(cn); err != nil {
		return nil, err
	}

	for {
		if _, err := writeCopyData(cn.Wr, r); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if err := cn.Wr.Flush(); err != nil {
			return nil, err
		}
	}

	writeCopyDone(cn.Wr)
	if err := cn.Wr.Flush(); err != nil {
		return nil, err
	}

	return readReadyForQueryOrError(cn)
}
