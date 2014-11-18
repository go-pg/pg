package pg_test

import (
	"net"
	"time"

	. "gopkg.in/check.v1"

	"gopkg.in/pg.v3"
)

var _ = Suite(&ListenerTest{})

type ListenerTest struct {
	db *pg.DB
	ln *pg.Listener
}

func (t *ListenerTest) SetUpTest(c *C) {
	t.db = pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "test",
		PoolSize: 2,
	})

	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)
	t.ln = ln
}

func (t *ListenerTest) TearDownTest(c *C) {
	c.Assert(t.db.Close(), IsNil)
}

func (t *ListenerTest) TestListenNotify(c *C) {
	_, err := t.db.Exec("NOTIFY test_channel")
	c.Assert(err, IsNil)

	channel, payload, err := t.ln.Receive()
	c.Assert(err, IsNil)
	c.Assert(channel, Equals, "test_channel")
	c.Assert(payload, Equals, "")
}

func (t *ListenerTest) TestCloseAbortsListener(c *C) {
	done := make(chan struct{})
	go func() {
		_, _, err := t.ln.Receive()
		c.Assert(err.Error(), Equals, "read tcp 127.0.0.1:5432: use of closed network connection")
		close(done)
	}()

	select {
	case <-done:
		c.Fail()
	case <-time.After(1 * time.Second):
		// ok
	}

	c.Assert(t.ln.Close(), IsNil)
	<-done
}

func (t *ListenerTest) TestListenTimeout(c *C) {
	channel, payload, err := t.ln.ReceiveTimeout(time.Second)
	c.Assert(err.(net.Error).Timeout(), Equals, true)
	c.Assert(channel, Equals, "")
	c.Assert(payload, Equals, "")
}

func (t *ListenerTest) TestReconnectOnListenError(c *C) {
	cn := t.ln.Conn()
	c.Assert(cn, Not(IsNil))
	c.Assert(cn.Close(), IsNil)

	err := t.ln.Listen("test_channel2")
	c.Assert(err.Error(), Equals, "use of closed network connection")

	err = t.ln.Listen("test_channel2")
	c.Assert(err, IsNil)
}

func (t *ListenerTest) TestReconnectOnReceiveError(c *C) {
	cn := t.ln.Conn()
	c.Assert(cn, Not(IsNil))
	c.Assert(cn.Close(), IsNil)

	_, _, err := t.ln.ReceiveTimeout(time.Second)
	c.Assert(err.Error(), Equals, "use of closed network connection")

	_, _, err = t.ln.ReceiveTimeout(time.Second)
	c.Assert(err.(net.Error).Timeout(), Equals, true)

	done := make(chan struct{})
	go func() {
		_, _, err := t.ln.Receive()
		c.Assert(err, IsNil)
		close(done)
	}()

	_, err = t.db.Exec("NOTIFY test_channel")
	c.Assert(err, IsNil)

	select {
	case <-done:
		// ok
	case <-time.After(1 * time.Second):
		c.Fail()
	}
}
