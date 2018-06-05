package pg

import (
	"errors"
	"sync"
	"time"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/pool"
	"github.com/go-pg/pg/types"
)

var errListenerClosed = errors.New("pg: listener is closed")

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
	cn     *pool.Conn
	closed bool
}

func (ln *Listener) conn(readTimeout time.Duration) (*pool.Conn, error) {
	ln.mu.Lock()
	cn, err := ln._conn(readTimeout)
	ln.mu.Unlock()
	if err != nil {
		if err == pool.ErrClosed {
			_ = ln.Close()
			return nil, errListenerClosed
		}
		if err != errListenerClosed {
			internal.Logf("pg: Listen failed: %s", err)
		}
		return nil, err
	}

	cn.SetTimeout(readTimeout, ln.db.opt.WriteTimeout)
	return cn, nil
}

func (ln *Listener) _conn(readTimeout time.Duration) (*pool.Conn, error) {
	if ln.closed {
		return nil, errListenerClosed
	}

	if ln.cn != nil {
		return ln.cn, nil
	}

	cn, err := ln.db.pool.NewConn()
	if err != nil {
		return nil, err
	}

	if cn.InitedAt.IsZero() {
		if err := ln.db.initConn(cn); err != nil {
			_ = ln.db.pool.CloseConn(cn)
			return nil, err
		}
		cn.InitedAt = time.Now()
	}

	if len(ln.channels) > 0 {
		if err := ln.listen(cn, ln.channels...); err != nil {
			_ = ln.db.pool.CloseConn(cn)
			return nil, err
		}
	}

	ln.cn = cn
	return cn, nil
}

func (ln *Listener) freeConn(cn *pool.Conn, err error) {
	if !isBadConn(err, true) {
		return
	}

	ln.mu.Lock()
	if cn == ln.cn {
		_ = ln.closeConn(err)
	}
	ln.mu.Unlock()
}

func (ln *Listener) closeConn(reason error) error {
	if !ln.closed {
		internal.Logf("pg: discarding bad listener connection: %s", reason)
	}

	err := ln.db.pool.CloseConn(ln.cn)
	ln.cn = nil
	return err
}

// Close closes the listener, releasing any open resources.
func (ln *Listener) Close() error {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	if ln.closed {
		return errListenerClosed
	}
	ln.closed = true
	if ln.cn != nil {
		return ln.closeConn(errListenerClosed)
	}
	return nil
}

// Channel returns a channel for concurrently receiving notifications.
// Receive can't be used after Channel is called.
//
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
				if isBadConn(err, true) {
					time.Sleep(100 * time.Millisecond)
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
			ln.freeConn(cn, err)
		}
		return err
	}

	ln.channels = appendIfNotExists(ln.channels, channels...)
	return nil
}

func (ln *Listener) listen(cn *pool.Conn, channels ...string) error {
	for _, channel := range channels {
		err := writeQueryMsg(cn.Writer, ln.db, "LISTEN ?", pgChan(channel))
		if err != nil {
			return err
		}
	}
	return cn.FlushWriter()
}

// Receive indefinitely waits for a notification.
func (ln *Listener) Receive() (channel string, payload string, err error) {
	return ln.ReceiveTimeout(0)
}

// ReceiveTimeout waits for a notification until timeout is reached.
func (ln *Listener) ReceiveTimeout(timeout time.Duration) (channel, payload string, err error) {
	cn, err := ln.conn(timeout)
	if err != nil {
		return "", "", err
	}

	channel, payload, err = readNotification(cn)
	if err != nil {
		ln.freeConn(cn, err)
		return "", "", err
	}

	return channel, payload, nil
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

type pgChan string

var _ types.ValueAppender = pgChan("")

func (ch pgChan) AppendValue(b []byte, quote int) []byte {
	if quote == 0 {
		return append(b, ch...)
	}

	b = append(b, '"')
	for _, c := range []byte(ch) {
		if c == '"' {
			b = append(b, '"', '"')
		} else {
			b = append(b, c)
		}
	}
	b = append(b, '"')

	return b
}
