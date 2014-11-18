package pg

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Not thread-safe.
type Listener struct {
	channels []string

	db *DB

	cnMtx sync.Mutex
	_cn   *conn

	_closed int64
}

func (l *Listener) conn(readTimeout time.Duration) (*conn, error) {
	if l.closed() {
		return nil, errListenerClosed
	}

	defer l.cnMtx.Unlock()
	l.cnMtx.Lock()

	if l._cn == nil {
		cn, err := l.db.conn()
		if err != nil {
			return nil, err
		}

		if len(l.channels) > 0 {
			if err := l.listen(cn, l.channels...); err != nil {
				return nil, err
			}
		}

		l._cn = cn
	}

	l._cn.SetReadTimeout(readTimeout)
	l._cn.SetWriteTimeout(l.db.opt.WriteTimeout)
	return l._cn, nil
}

func (l *Listener) Listen(channels ...string) error {
	cn, err := l.conn(l.db.opt.ReadTimeout)
	if err != nil {
		return err
	}
	if err := l.listen(cn, channels...); err != nil {
		if canRetry(err) {
			l.discardConn()
		}
		return err
	}
	l.channels = append(l.channels, channels...)
	return nil
}

func (l *Listener) listen(cn *conn, channels ...string) error {
	for _, channel := range channels {
		if err := writeQueryMsg(cn.buf, "LISTEN ?", F(channel)); err != nil {
			return err
		}
	}
	return cn.Flush()
}

func (l *Listener) Receive() (channel string, payload string, err error) {
	return l.ReceiveTimeout(0)
}

func (l *Listener) ReceiveTimeout(readTimeout time.Duration) (channel, payload string, err error) {
	channel, payload, err = l.receiveTimeout(readTimeout)
	if canRetry(err) {
		l.discardConn()
	}
	return channel, payload, err
}

func (l *Listener) receiveTimeout(readTimeout time.Duration) (channel, payload string, err error) {
	cn, err := l.conn(readTimeout)
	if err != nil {
		return "", "", err
	}

	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return "", "", err
		}

		switch c {
		case commandCompleteMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return "", "", err
			}
		case readyForQueryMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return "", "", err
			}
		case errorResponseMsg:
			e, err := cn.ReadError()
			if err != nil {
				return "", "", err
			}
			return "", "", e
		case notificationResponseMsg:
			_, err := cn.ReadInt32()
			if err != nil {
				return "", "", err
			}
			channel, err = cn.ReadString()
			if err != nil {
				return "", "", err
			}
			payload, err = cn.ReadString()
			if err != nil {
				return "", "", err
			}
			return channel, payload, nil
		default:
			return "", "", fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}

func (l *Listener) discardConn() (err error) {
	l.cnMtx.Lock()
	if l._cn != nil {
		err = l.db.pool.Remove(l._cn)
		l._cn = nil
	}
	l.cnMtx.Unlock()
	return err
}

func (l *Listener) Close() error {
	if !atomic.CompareAndSwapInt64(&l._closed, 0, 1) {
		return errListenerClosed
	}
	return l.discardConn()
}

func (l *Listener) closed() bool {
	return atomic.LoadInt64(&l._closed) == 1
}
