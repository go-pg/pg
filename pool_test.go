package pg_test

import (
	"net"
	"sync"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"gopkg.in/pg.v2"
)

func TestCancelRequestOnTimeout(t *testing.T) {
	db := pg.Connect(&pg.Options{
		User:        "postgres",
		Database:    "test",
		ReadTimeout: time.Second,
	})
	defer db.Close()

	_, err := db.Exec("SELECT pg_sleep(60)")
	if err == nil {
		t.Errorf("err is nil")
	}
	neterr, ok := err.(net.Error)
	if !ok {
		t.Errorf("got %v, expected net.Error", err)
	}
	if !neterr.Timeout() {
		t.Errorf("got %v, expected timeout", err)
	}

	if db.Pool().Size() != 0 || db.Pool().Len() != 0 {
		t.Errorf("pool is not empty")
	}

	// Give PostgreSQL some time to cancel request.
	time.Sleep(time.Second)

	testNoActivity(t, db)
}

func TestStatementTimeout(t *testing.T) {
	db := pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "test",

		Params: map[string]interface{}{
			"statement_timeout": 1000,
		},
	})
	defer db.Close()

	_, err := db.Exec("SELECT pg_sleep(60)")
	if err == nil {
		t.Errorf("err is nil")
	}
	if err.Error() != "ERROR #57014 canceling statement due to statement timeout: " {
		t.Errorf("got %q", err.Error())
	}

	if db.Pool().Size() != 1 || db.Pool().Len() != 1 {
		t.Errorf("pool is empty")
	}

	// Give PostgreSQL some time to cancel request.
	time.Sleep(time.Second)

	testNoActivity(t, db)
}

func testNoActivity(t *testing.T, db *pg.DB) {
	var queries pg.Strings
	_, err := db.Query(&queries, `
		SELECT query FROM pg_stat_activity WHERE datname = 'test'
	`)
	if err != nil {
		t.Error(err)
	}
	if len(queries) > 1 {
		t.Errorf("there are active queries running: %v", queries)
	}
}

var _ = Suite(&PoolTest{})

type PoolTest struct {
	db *pg.DB
}

func (t *PoolTest) SetUpTest(c *C) {
	t.db = pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "test",
		PoolSize: 10,

		IdleTimeout:        time.Second,
		IdleCheckFrequency: time.Second,
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

func (t *PoolTest) TestCloseClosesAllConnections(c *C) {
	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		_, _, err := ln.Receive()
		c.Assert(err, Not(IsNil))
		c.Assert(err.Error(), Equals, "read tcp 127.0.0.1:5432: use of closed network connection")
		close(done)
	}()

	<-started
	c.Assert(t.db.Close(), IsNil)
	<-done

	c.Assert(t.db.Pool().Size(), Equals, 0)
	c.Assert(t.db.Pool().Len(), Equals, 0)
}

func (t *PoolTest) TestClosedDB(c *C) {
	c.Assert(t.db.Close(), IsNil)

	c.Assert(t.db.Pool().Size(), Equals, 0)
	c.Assert(t.db.Pool().Len(), Equals, 0)

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

	c.Assert(t.db.Pool().Size(), Equals, 1)
	c.Assert(t.db.Pool().Len(), Equals, 0)

	c.Assert(ln.Close(), IsNil)

	c.Assert(t.db.Pool().Size(), Equals, 0)
	c.Assert(t.db.Pool().Len(), Equals, 0)

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

	c.Assert(t.db.Pool().Size(), Equals, 1)
	c.Assert(t.db.Pool().Len(), Equals, 0)

	c.Assert(tx.Rollback(), IsNil)

	c.Assert(t.db.Pool().Size(), Equals, 1)
	c.Assert(t.db.Pool().Len(), Equals, 1)

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

	c.Assert(t.db.Pool().Size(), Equals, 1)
	c.Assert(t.db.Pool().Len(), Equals, 0)

	c.Assert(stmt.Close(), IsNil)

	c.Assert(t.db.Pool().Size(), Equals, 1)
	c.Assert(t.db.Pool().Len(), Equals, 1)

	err = stmt.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: statement is closed")

	_, err = stmt.Exec(1)
	c.Assert(err.Error(), Equals, "pg: statement is closed")
}

func (t *PoolTest) TestIdleConnectionsAreClosed(c *C) {
	_, err := t.db.Exec("SELECT 1")
	c.Assert(err, IsNil)

	c.Assert(t.db.Pool().Size(), Equals, 1)
	c.Assert(t.db.Pool().Len(), Equals, 1)

	// Give pg time to close idle connections.
	time.Sleep(2 * time.Second)

	c.Assert(t.db.Pool().Size(), Equals, 0)
	c.Assert(t.db.Pool().Len(), Equals, 0)
}
