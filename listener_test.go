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
	opt := pgOptions()
	opt.PoolSize = 2
	opt.PoolTimeout = time.Second
	t.db = pg.Connect(opt)

	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)
	t.ln = ln
}

func (t *ListenerTest) TearDownTest(c *C) {
	c.Assert(t.db.Close(), IsNil)
}

func (t *ListenerTest) TestListenNotify(c *C) {
	wait := make(chan struct{}, 2)
	go func() {
		wait <- struct{}{}
		channel, payload, err := t.ln.Receive()
		c.Assert(err, IsNil)
		c.Assert(channel, Equals, "test_channel")
		c.Assert(payload, Equals, "")
		wait <- struct{}{}
	}()

	select {
	case <-wait:
		// ok
	case <-time.After(3 * time.Second):
		c.Fatal("timeout")
	}

	_, err := t.db.Exec("NOTIFY test_channel")
	c.Assert(err, IsNil)

	select {
	case <-wait:
		// ok
	case <-time.After(3 * time.Second):
		c.Fatal("timeout")
	}
}

func (t *ListenerTest) TestCloseAbortsListener(c *C) {
	wait := make(chan struct{}, 2)

	go func() {
		wait <- struct{}{}
		_, _, err := t.ln.Receive()
		c.Assert(err, ErrorMatches, `^(.*use of closed network connection|EOF)$`)
		wait <- struct{}{}
	}()

	select {
	case <-wait:
		// ok
	case <-time.After(3 * time.Second):
		c.Fatal("timeout")
	}

	select {
	case <-wait:
		c.Fatal("Receive is not blocked")
	case <-time.After(time.Second):
		// ok
	}

	c.Assert(t.ln.Close(), IsNil)

	select {
	case <-wait:
		// ok
	case <-time.After(3 * time.Second):
		c.Fatal("timeout")
	}
}

func (t *ListenerTest) TestListenTimeout(c *C) {
	channel, payload, err := t.ln.ReceiveTimeout(time.Second)
	c.Assert(err.(net.Error).Timeout(), Equals, true)
	c.Assert(channel, Equals, "")
	c.Assert(payload, Equals, "")
}

func (t *ListenerTest) TestReconnectOnListenError(c *C) {
	cn := t.ln.CurrentConn()
	c.Assert(cn, Not(IsNil))
	c.Assert(cn.Close(), IsNil)

	err := t.ln.Listen("test_channel2")
	c.Assert(err, ErrorMatches, `^(.*use of closed network connection|EOF)$`)

	err = t.ln.Listen("test_channel2")
	c.Assert(err, IsNil)
}

func (t *ListenerTest) TestReconnectOnReceiveError(c *C) {
	cn := t.ln.CurrentConn()
	c.Assert(cn, Not(IsNil))
	c.Assert(cn.Close(), IsNil)

	_, _, err := t.ln.ReceiveTimeout(time.Second)
	c.Assert(err, ErrorMatches, `^(.*use of closed network connection|EOF)$`)

	_, _, err = t.ln.ReceiveTimeout(time.Second)
	c.Assert(err.(net.Error).Timeout(), Equals, true)

	wait := make(chan struct{}, 2)
	go func() {
		wait <- struct{}{}
		_, _, err := t.ln.Receive()
		c.Assert(err, IsNil)
		wait <- struct{}{}
	}()

	select {
	case <-wait:
		// ok
	case <-time.After(3 * time.Second):
		c.Fatal("timeout")
	}

	select {
	case <-wait:
		c.Fatal("Receive is not blocked")
	case <-time.After(time.Second):
		// ok
	}

	_, err = t.db.Exec("NOTIFY test_channel")
	c.Assert(err, IsNil)

	select {
	case <-wait:
		// ok
	case <-time.After(3 * time.Second):
		c.Fatal("timeout")
	}
}
