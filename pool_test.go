package pg_test

import (
	"net"
	"sync"
	"time"

	. "launchpad.net/gocheck"

	"github.com/vmihailenco/pg"
)

var _ = Suite(&PoolTest{})

type PoolTest struct {
	db *pg.DB
}

func (t *PoolTest) SetUpTest(c *C) {
	t.db = pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "test",
		PoolSize: 10,

		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
}

func (t *PoolTest) TearDownTest(c *C) {
	t.db.Close()
}

func (t *PoolTest) TestPoolReusesConnection(c *C) {
	for i := 0; i < 100; i++ {
		_, err := t.db.Exec("SELECT 1")
		c.Assert(err, IsNil)
	}

	c.Assert(t.db.Pool().Size(), Equals, 1)
	c.Assert(t.db.Pool().Len(), Equals, 1)
}

func (t *PoolTest) TestPoolMaxSize(c *C) {
	N := 1000

	wg := &sync.WaitGroup{}
	wg.Add(N)
	for i := 0; i < 1000; i++ {
		go func() {
			_, err := t.db.Exec("SELECT 1")
			c.Assert(err, IsNil)
			wg.Done()
		}()
	}
	wg.Wait()

	c.Assert(t.db.Pool().Size(), Equals, 10)
	c.Assert(t.db.Pool().Len(), Equals, 10)
}

func (t *PoolTest) TestTimeoutAndCancelRequest(c *C) {
	_, err := t.db.Exec("SELECT pg_sleep(60)")
	c.Assert(err.(net.Error).Timeout(), Equals, true)

	c.Assert(t.db.Pool().Size(), Equals, 0)
	c.Assert(t.db.Pool().Len(), Equals, 0)

	// Give PostgreSQL some time to cancel request.
	time.Sleep(time.Second)

	// Unreliable check that previous query was cancelled.
	var count int
	_, err = t.db.QueryOne(pg.LoadInto(&count), "SELECT COUNT(*) FROM pg_stat_activity")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)
}

func (t *PoolTest) TestCloseClosesAllConnections(c *C) {
	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		_, _, err := ln.ReceiveTimeout(0)
		c.Assert(err, Not(IsNil))
		c.Assert(err.Error(), Equals, "read tcp 127.0.0.1:5432: use of closed network connection")
		close(done)
	}()

	<-started
	c.Assert(t.db.Close(), IsNil)
	<-done
}

func (t *PoolTest) TestClosedDB(c *C) {
	c.Assert(t.db.Close(), IsNil)

	err := t.db.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: database is closed")

	_, err = t.db.Exec("SELECT 1")
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: database is closed")
}

func (t *PoolTest) TestClosedListener(c *C) {
	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)

	c.Assert(ln.Close(), IsNil)

	err = ln.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: listener is closed")

	_, _, err = ln.Receive()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: listener is closed")
}

func (t *PoolTest) TestClosedTx(c *C) {
	tx, err := t.db.Begin()
	c.Assert(err, IsNil)

	c.Assert(tx.Rollback(), IsNil)

	err = tx.Rollback()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: transaction has already been committed or rolled back")

	_, err = tx.Exec("SELECT 1")
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: transaction has already been committed or rolled back")
}

func (t *PoolTest) TestClosedStmt(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::int")
	c.Assert(err, IsNil)

	c.Assert(stmt.Close(), IsNil)

	err = stmt.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: statement is closed")

	_, err = stmt.Exec(1)
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: statement is closed")
}
