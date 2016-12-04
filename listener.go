package pg

import (
	"sync"
	"time"

	"gopkg.in/pg.v5/internal"
	"gopkg.in/pg.v5/internal/pool"
)

// A notification received with LISTEN command.
type Notification struct {
	Channel string
	Payload string
}

// Listener listens for notifications sent with NOTIFY command.
// It's NOT safe for concurrent use by multiple goroutines
// except the Channel API.
type Listener struct {
	db *DB

	channels []string

	mu     sync.Mutex
	_cn    *pool.Conn
	closed bool
}

func (ln *Listener) conn(readTimeout time.Duration) (*pool.Conn, error) {
	defer ln.mu.Unlock()
	ln.mu.Lock()

	if ln.closed {
		return nil, errListenerClosed
	}

	if ln._cn == nil {
		cn, err := ln.db.conn()
		if err != nil {
			return nil, err
		}
		ln._cn = cn

		if len(ln.channels) > 0 {
			if err := ln.listen(cn, ln.channels...); err != nil {
				return nil, err
			}
		}
	}

	ln._cn.SetReadWriteTimeout(readTimeout, ln.db.opt.WriteTimeout)
	return ln._cn, nil
}

// Channel returns a channel for concurrently receiving notifications.
// The channel is closed with Listener.
func (ln *Listener) Channel() <-chan *Notification {
	ch := make(chan *Notification, 100)
	go func() {
		for {
			channel, payload, err := ln.ReceiveTimeout(5 * time.Second)
			if err != nil {
				if err == errListenerClosed {
					break
				}
				continue
			}
			ch <- &Notification{channel, payload}
		}
		close(ch)
	}()
	return ch
}

// Listen starts listening for notifications on channels.
func (ln *Listener) Listen(channels ...string) error {
	cn, err := ln.conn(ln.db.opt.ReadTimeout)
	if err != nil {
		return err
	}

	if err := ln.listen(cn, channels...); err != nil {
		if err != nil {
			ln.freeConn(err)
		}
		return err
	}

	ln.channels = appendIfNotExists(ln.channels, channels...)
	return nil
}

func (ln *Listener) listen(cn *pool.Conn, channels ...string) error {
	for _, channel := range channels {
		if err := writeQueryMsg(cn.Wr, ln.db, "LISTEN ?", F(channel)); err != nil {
			return err
		}
	}
	return cn.Wr.Flush()
}

// Receive indefinitely waits for a notification.
func (ln *Listener) Receive() (channel string, payload string, err error) {
	return ln.ReceiveTimeout(0)
}

// ReceiveTimeout waits for a notification until timeout is reached.
func (ln *Listener) ReceiveTimeout(timeout time.Duration) (channel, payload string, err error) {
	channel, payload, err = ln.receiveTimeout(timeout)
	if err != nil {
		ln.freeConn(err)
	}
	return channel, payload, err
}

func (ln *Listener) receiveTimeout(readTimeout time.Duration) (channel, payload string, err error) {
	cn, err := ln.conn(readTimeout)
	if err != nil {
		return "", "", err
	}
	return readNotification(cn)
}

func (ln *Listener) freeConn(err error) (retErr error) {
	if !isBadConn(err, true) {
		return nil
	}
	return ln.closeConn(err)
}

func (ln *Listener) closeConn(reason error) error {
	var firstErr error

	ln.mu.Lock()
	if ln._cn != nil {
		if !ln.closed {
			internal.Logf("pg: discarding bad listener connection: %s", reason)
		}

		firstErr = ln.db.pool.Remove(ln._cn, reason)
		ln._cn = nil
	}
	ln.mu.Unlock()

	return firstErr
}

// Close closes the listener, releasing any open resources.
func (ln *Listener) Close() error {
	ln.mu.Lock()
	closed := ln.closed
	ln.closed = true
	ln.mu.Unlock()
	if closed {
		return errListenerClosed
	}
	return ln.closeConn(errListenerClosed)
}

func appendIfNotExists(ss []string, es ...string) []string {
loop:
	for _, e := range es {
		for _, s := range ss {
			if s == e {
				continue loop
			}
		}
		ss = append(ss, e)
	}
	return ss
}
