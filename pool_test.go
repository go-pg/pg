package pg_test

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"gopkg.in/pg.v3"
)

func TestCancelRequestOnTimeout(t *testing.T) {
	opt := pgOptions()
	opt.ReadTimeout = time.Second
	db := pg.Connect(opt)
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

	if db.Pool().FreeLen() != 1 {
		t.Errorf("len is %d", db.Pool().FreeLen())
	}
	if db.Pool().Len() != 1 {
		t.Errorf("size is %d", db.Pool().Len())
	}

	err = eventually(func() error {
		return verifyNoActivity(db)
	}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStatementTimeout(t *testing.T) {
	opt := pgOptions()
	opt.Params = map[string]interface{}{
		"statement_timeout": 1000,
	}
	db := pg.Connect(opt)
	defer db.Close()

	_, err := db.Exec("SELECT pg_sleep(60)")
	if err == nil {
		t.Errorf("err is nil")
	}
	if err.Error() != "ERROR #57014 canceling statement due to statement timeout: " {
		t.Errorf("got %q", err.Error())
	}

	if db.Pool().Len() != 1 || db.Pool().FreeLen() != 1 {
		t.Errorf("pool is empty")
	}

	err = eventually(func() error {
		return verifyNoActivity(db)
	}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func verifyNoActivity(db *pg.DB) error {
	var queries pg.Strings
	_, err := db.Query(&queries, `
		SELECT query FROM pg_stat_activity WHERE datname = 'test'
	`)
	if err != nil {
		return err
	}
	if len(queries) > 1 {
		return fmt.Errorf("there are %d active queries running: %#v", len(queries), queries)
	}
	return nil
}

var _ = Suite(&PoolTest{})

type PoolTest struct {
	db *pg.DB
}

func (t *PoolTest) SetUpTest(c *C) {
	opt := pgOptions()
	opt.IdleTimeout = time.Second
	opt.IdleCheckFrequency = time.Second
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
	c.Assert(t.db.Pool().FreeLen(), Equals, 1)
}

func (t *PoolTest) TestPoolMaxSize(c *C) {
	N := 1000

	wg := &sync.WaitGroup{}
	wg.Add(N)
	for i := 0; i < 1000; i++ {
		go func() {
			_, err := t.db.Exec("SELECT 'test_pool_max_size'")
			c.Assert(err, IsNil)
			wg.Done()
		}()
	}

	wait := make(chan struct{}, 2)
	go func() {
		wg.Wait()
		wait <- struct{}{}
	}()

	select {
	case <-wait:
		// ok
	case <-time.After(3 * time.Second):
		c.Fatal("timeout")
	}

	c.Assert(t.db.Pool().Len(), Equals, 10)
	c.Assert(t.db.Pool().FreeLen(), Equals, 10)
}

func (t *PoolTest) TestCloseClosesAllConnections(c *C) {
	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)

	wait := make(chan struct{}, 2)
	go func() {
		wait <- struct{}{}
		_, _, err := ln.Receive()
		c.Assert(err, ErrorMatches, `^(.*use of closed network connection|EOF)$`)
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
	c.Assert(t.db.Pool().FreeLen(), Equals, 0)
}

func (t *PoolTest) TestClosedDB(c *C) {
	c.Assert(t.db.Close(), IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 0)
	c.Assert(t.db.Pool().FreeLen(), Equals, 0)

	err := t.db.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: database is closed")

	_, err = t.db.Exec("SELECT 'test_closed_db'")
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: database is closed")
}

func (t *PoolTest) TestClosedListener(c *C) {
	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().FreeLen(), Equals, 0)

	c.Assert(ln.Close(), IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().FreeLen(), Equals, 1)

	err = ln.Close()
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
	c.Assert(t.db.Pool().FreeLen(), Equals, 0)

	c.Assert(tx.Rollback(), IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().FreeLen(), Equals, 1)

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
	c.Assert(t.db.Pool().FreeLen(), Equals, 0)

	c.Assert(stmt.Close(), IsNil)

	c.Assert(t.db.Pool().Len(), Equals, 1)
	c.Assert(t.db.Pool().FreeLen(), Equals, 1)

	err = stmt.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: statement is closed")

	_, err = stmt.Exec(1)
	c.Assert(err.Error(), Equals, "pg: statement is closed")
}

func eventually(fn func() error, timeout time.Duration) (err error) {
	done := make(chan struct{})
	var exit int32
	go func() {
		for atomic.LoadInt32(&exit) == 0 {
			err = fn()
			if err == nil {
				close(done)
				return
			}
			time.Sleep(timeout / 100)
		}
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		atomic.StoreInt32(&exit, 1)
		return err
	}
}
