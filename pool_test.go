package pg_test

import (
	"time"

	"github.com/go-pg/pg"

	. "gopkg.in/check.v1"
)

var _ = Suite(&PoolTest{})

type PoolTest struct {
	db *pg.DB
}

func (t *PoolTest) SetUpTest(c *C) {
	opt := pgOptions()
	opt.IdleTimeout = time.Second
	t.db = pg.Connect(opt)
}

func (t *PoolTest) TearDownTest(c *C) {
	_ = t.db.Close()
}

func (t *PoolTest) TestPoolReusesConnection(c *C) {
	for i := 0; i < 100; i++ {
		_, err := t.db.Exec("SELECT 'test_pool_reuses_connection'")
		c.Assert(err, IsNil)
	}

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().IdleLen(), Equals, 1)
}

func (t *PoolTest) TestPoolMaxSize(c *C) {
	N := 1000

	perform(N, func(int) {
		_, err := t.db.Exec("SELECT 'test_pool_max_size'")
		c.Assert(err, IsNil)
	})

	c.Assert(t.db.Pool().Len(), Equals, 10)
	c.Assert(t.db.Pool().IdleLen(), Equals, 10)
}

func (t *PoolTest) TestCloseClosesAllConnections(c *C) {
	ln := t.db.Listen("test_channel")

	wait := make(chan struct{}, 2)
	go func() {
		wait <- struct{}{}
		_, _, err := ln.Receive()
		c.Assert(err, ErrorMatches, `^(.*use of closed (file or )?network connection|EOF)$`)
		wait <- struct{}{}
	}()

	select {
	case <-wait:
		// ok
	case <-time.After(3 * time.Second):
		c.Fatal("timeout")
	}

	c.Assert(t.db.Close(), IsNil)

	select {
	case <-wait:
		// ok
	case <-time.After(3 * time.Second):
		c.Fatal("timeout")
	}

	c.Assert(t.db.Pool().Len(), Equals, 0)
	c.Assert(t.db.Pool().IdleLen(), Equals, 0)
}

func (t *PoolTest) TestClosedDB(c *C) {
	c.Assert(t.db.Close(), IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 0)
	c.Assert(t.db.Pool().IdleLen(), Equals, 0)

	err := t.db.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: database is closed")

	_, err = t.db.Exec("SELECT 'test_closed_db'")
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: database is closed")
}

func (t *PoolTest) TestClosedListener(c *C) {
	ln := t.db.Listen("test_channel")

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().IdleLen(), Equals, 0)

	c.Assert(ln.Close(), IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 0)
	c.Assert(t.db.Pool().IdleLen(), Equals, 0)

	err := ln.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: listener is closed")

	_, _, err = ln.ReceiveTimeout(time.Second)
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: listener is closed")
}

func (t *PoolTest) TestClosedTx(c *C) {
	tx, err := t.db.Begin()
	c.Assert(err, IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().IdleLen(), Equals, 0)

	c.Assert(tx.Rollback(), IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().IdleLen(), Equals, 1)

	err = tx.Rollback()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: transaction has already been committed or rolled back")

	_, err = tx.Exec("SELECT 'test_closed_tx'")
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: transaction has already been committed or rolled back")
}

func (t *PoolTest) TestClosedStmt(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::int")
	c.Assert(err, IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().IdleLen(), Equals, 0)

	c.Assert(stmt.Close(), IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().IdleLen(), Equals, 1)

	err = stmt.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: statement is closed")

	_, err = stmt.Exec(1)
	c.Assert(err.Error(), Equals, "pg: statement is closed")
}
