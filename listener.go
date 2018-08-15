package pg

import (
	"errors"
	"sync"
	"time"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/internal/pool"
	"github.com/go-pg/pg/types"
)

const gopgChannel = "gopg:ping"

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
	exit   chan struct{}

	chOnce sync.Once
	ch     chan *Notification
	pingCh chan struct{}
}

func (ln *Listener) init() {
	ln.exit = make(chan struct{})
}

func (ln *Listener) conn() (*pool.Conn, error) {
	ln.mu.Lock()
	cn, err := ln._conn()
	ln.mu.Unlock()

	switch err {
	case nil:
		return cn, nil
	case errListenerClosed:
		return nil, err
	case pool.ErrClosed:
		_ = ln.Close()
		return nil, errListenerClosed
	default:
		internal.Logf("pg: Listen failed: %s", err)
		return nil, err
	}
}

func (ln *Listener) _conn() (*pool.Conn, error) {
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
	cn.LockReaderBuffer()

	if cn.InitedAt.IsZero() {
		err := ln.db.initConn(cn)
		if err != nil {
			_ = ln.db.pool.CloseConn(cn)
			return nil, err
		}
		cn.InitedAt = time.Now()
	}

	if len(ln.channels) > 0 {
		err := ln.listen(cn, ln.channels...)
		if err != nil {
			_ = ln.db.pool.CloseConn(cn)
			return nil, err
		}
	}

	ln.cn = cn
	return cn, nil
}

func (ln *Listener) releaseConn(cn *pool.Conn, err error, allowTimeout bool) {
	ln.mu.Lock()
	if ln.cn == cn {
		if isBadConn(err, allowTimeout) {
			ln._reconnect(err)
		}
	}
	ln.mu.Unlock()
}

func (ln *Listener) _closeTheCn(reason error) error {
	if ln.cn == nil {
		return nil
	}
	if !ln.closed {
		internal.Logf("pg: discarding bad listener connection: %s", reason)
	}

	err := ln.db.pool.CloseConn(ln.cn)
	ln.cn = nil
	return err
}

func (ln *Listener) _reconnect(reason error) {
	_ = ln._closeTheCn(reason)
	_, _ = ln._conn()
}

// Close closes the listener, releasing any open resources.
func (ln *Listener) Close() error {
	ln.mu.Lock()
	defer ln.mu.Unlock()

	if ln.closed {
		return errListenerClosed
	}
	ln.closed = true
	close(ln.exit)

	return ln._closeTheCn(errListenerClosed)
}

// Listen starts listening for notifications on channels.
func (ln *Listener) Listen(channels ...string) error {
	cn, err := ln.conn()
	if err != nil {
		return err
	}

	err = ln.listen(cn, channels...)
	if err != nil {
		ln.releaseConn(cn, err, false)
		return err
	}

	ln.channels = appendIfNotExists(ln.channels, channels...)
	return nil
}

func (ln *Listener) listen(cn *pool.Conn, channels ...string) error {
	err := cn.WithWriter(ln.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		for _, channel := range channels {
			err := writeQueryMsg(wb, ln.db, "LISTEN ?", pgChan(channel))
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Receive indefinitely waits for a notification. This is low-level API
// and in most cases Channel should be used instead.
func (ln *Listener) Receive() (channel string, payload string, err error) {
	return ln.ReceiveTimeout(0)
}

// ReceiveTimeout waits for a notification until timeout is reached.
// This is low-level API and in most cases Channel should be used instead.
func (ln *Listener) ReceiveTimeout(timeout time.Duration) (channel, payload string, err error) {
	cn, err := ln.conn()
	if err != nil {
		return "", "", err
	}

	err = cn.WithReader(timeout, func(rd *pool.Reader) error {
		channel, payload, err = readNotification(rd)
		return err
	})
	if err != nil {
		ln.releaseConn(cn, err, timeout > 0)
		return "", "", err
	}

	return channel, payload, nil
}

// Channel returns a channel for concurrently receiving notifications.
// It periodically sends Ping messages to test connection health.
//
// The channel is closed with Listener. Receive* APIs can not be used
// after channel is created.
func (ln *Listener) Channel() <-chan *Notification {
	ln.chOnce.Do(ln.initChannel)
	return ln.ch
}

func (ln *Listener) initChannel() {
	_ = ln.Listen(gopgChannel)

	ln.ch = make(chan *Notification, 100)
	ln.pingCh = make(chan struct{}, 10)

	go func() {
		var errCount int
		for {
			channel, payload, err := ln.Receive()
			if err != nil {
				if err == errListenerClosed {
					close(ln.ch)
					return
				}
				if errCount > 0 {
					time.Sleep(ln.db.retryBackoff(errCount))
				}
				errCount++
				continue
			}
			errCount = 0

			// Any message is as good as a ping.
			select {
			case ln.pingCh <- struct{}{}:
			default:
			}

			switch channel {
			case gopgChannel:
				// ignore
			default:
				ln.ch <- &Notification{channel, payload}
			}
		}
	}()

	go func() {
		const timeout = 5 * time.Second

		timer := time.NewTimer(timeout)
		timer.Stop()

		healthy := true
		var pingErr error
		for {
			timer.Reset(timeout)
			select {
			case <-ln.pingCh:
				healthy = true
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				pingErr = ln.ping()
				if healthy {
					healthy = false
				} else {
					ln.mu.Lock()
					ln._reconnect(pingErr)
					ln.mu.Unlock()
				}
			case <-ln.exit:
				return
			}
		}
	}()
}

func (ln *Listener) ping() error {
	_, err := ln.db.Exec("NOTIFY ?", pgChan(gopgChannel))
	return err
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
