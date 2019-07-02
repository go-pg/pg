package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-pg/pg/v9/internal"
	"github.com/go-pg/pg/v9/internal/pool"
	"github.com/go-pg/pg/v9/types"
)

const gopgChannel = "gopg:ping"

var errListenerClosed = errors.New("pg: listener is closed")
var errPingTimeout = errors.New("pg: ping timeout")

// Notification which is received with LISTEN command.
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
	exit   chan struct{}
	closed bool

	chOnce sync.Once
	ch     chan *Notification
	pingCh chan struct{}
}

func (ln *Listener) String() string {
	return fmt.Sprintf("Listener(%s)", strings.Join(ln.channels, ", "))
}

func (ln *Listener) init() {
	ln.exit = make(chan struct{})
}

func (ln *Listener) connWithLock() (*pool.Conn, error) {
	ln.mu.Lock()
	cn, err := ln.conn()
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
		internal.Logger.Printf("pg: Listen failed: %s", err)
		return nil, err
	}
}

func (ln *Listener) conn() (*pool.Conn, error) {
	if ln.closed {
		return nil, errListenerClosed
	}

	if ln.cn != nil {
		return ln.cn, nil
	}

	c := context.TODO()

	cn, err := ln.db.pool.NewConn(c)
	if err != nil {
		return nil, err
	}

	err = ln.db.initConn(c, cn)
	if err != nil {
		_ = ln.db.pool.CloseConn(cn)
		return nil, err
	}

	if len(ln.channels) > 0 {
		err := ln.listen(c, cn, ln.channels...)
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
			ln.reconnect(err)
		}
	}
	ln.mu.Unlock()
}

func (ln *Listener) reconnect(reason error) {
	_ = ln.closeTheCn(reason)
	_, _ = ln.conn()
}

func (ln *Listener) closeTheCn(reason error) error {
	if ln.cn == nil {
		return nil
	}
	if !ln.closed {
		internal.Logger.Printf("pg: discarding bad listener connection: %s", reason)
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
	close(ln.exit)

	return ln.closeTheCn(errListenerClosed)
}

// Listen starts listening for notifications on channels.
func (ln *Listener) Listen(channels ...string) error {
	// Always append channels so DB.Listen works correctly.
	ln.channels = appendIfNotExists(ln.channels, channels...)

	cn, err := ln.connWithLock()
	if err != nil {
		return err
	}

	err = ln.listen(context.TODO(), cn, channels...)
	if err != nil {
		ln.releaseConn(cn, err, false)
		return err
	}

	return nil
}

func (ln *Listener) listen(c context.Context, cn *pool.Conn, channels ...string) error {
	err := cn.WithWriter(c, ln.db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		for _, channel := range channels {
			err := writeQueryMsg(wb, ln.db.fmter, "LISTEN ?", pgChan(channel))
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
	cn, err := ln.connWithLock()
	if err != nil {
		return "", "", err
	}

	err = cn.WithReader(context.TODO(), timeout, func(rd *internal.BufReader) error {
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
// It periodically sends Ping notification to test connection health.
//
// The channel is closed with Listener. Receive* APIs can not be used
// after channel is created.
func (ln *Listener) Channel() <-chan *Notification {
	return ln.channel(100)
}

// ChannelSize is like Channel, but creates a Go channel
// with specified buffer size.
func (ln *Listener) ChannelSize(size int) <-chan *Notification {
	return ln.channel(size)
}

func (ln *Listener) channel(size int) <-chan *Notification {
	ln.chOnce.Do(func() {
		ln.initChannel(size)
	})
	if cap(ln.ch) != size {
		err := fmt.Errorf("pg: Listener.Channel is called with different buffer size")
		panic(err)
	}
	return ln.ch
}

func (ln *Listener) initChannel(size int) {
	const timeout = 30 * time.Second

	_ = ln.Listen(gopgChannel)

	ln.ch = make(chan *Notification, size)
	ln.pingCh = make(chan struct{}, 1)

	go func() {
		timer := time.NewTimer(timeout)
		timer.Stop()

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

			// Any notification is as good as a ping.
			select {
			case ln.pingCh <- struct{}{}:
			default:
			}

			switch channel {
			case gopgChannel:
				// ignore
			default:
				timer.Reset(timeout)
				select {
				case ln.ch <- &Notification{channel, payload}:
					if !timer.Stop() {
						<-timer.C
					}
				case <-timer.C:
					internal.Logger.Printf(
						"pg: %s channel is full for %s (notification is dropped)",
						ln, timeout)
				}
			}
		}
	}()

	go func() {
		timer := time.NewTimer(timeout)
		timer.Stop()

		healthy := true
		for {
			timer.Reset(timeout)
			select {
			case <-ln.pingCh:
				healthy = true
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				pingErr := ln.ping()
				if healthy {
					healthy = false
				} else {
					if pingErr == nil {
						pingErr = errPingTimeout
					}
					ln.mu.Lock()
					ln.reconnect(pingErr)
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

func (ch pgChan) AppendValue(b []byte, quote int) ([]byte, error) {
	if quote == 0 {
		return append(b, ch...), nil
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

	return b, nil
}
