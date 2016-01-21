package pg

import (
	"log"
	"sync"
	"time"
)

// Not thread-safe.
type Listener struct {
	channels []string

	db *DB

	_cn    *conn
	closed bool
	mx     sync.Mutex
}

func (l *Listener) conn(readTimeout time.Duration) (*conn, error) {
	defer l.mx.Unlock()
	l.mx.Lock()

	if l.closed {
		return nil, errListenerClosed
	}

	if l._cn == nil {
		cn, err := l.db.conn()
		if err != nil {
			return nil, err
		}
		l._cn = cn

		if len(l.channels) > 0 {
			if err := l.listen(cn, l.channels...); err != nil {
				return nil, err
			}
		}
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
		if err != nil {
			l.freeConn(err)
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
	return cn.FlushWrite()
}

func (l *Listener) Receive() (channel string, payload string, err error) {
	return l.ReceiveTimeout(0)
}

func (l *Listener) ReceiveTimeout(readTimeout time.Duration) (channel, payload string, err error) {
	channel, payload, err = l.receiveTimeout(readTimeout)
	if err != nil {
		l.freeConn(err)
	}
	return channel, payload, err
}

func (l *Listener) receiveTimeout(readTimeout time.Duration) (channel, payload string, err error) {
	cn, err := l.conn(readTimeout)
	if err != nil {
		return "", "", err
	}
	return readNotification(cn)
}

func (l *Listener) freeConn(err error) (retErr error) {
	if err != nil {
		if !canRetry(err) {
			return nil
		}
		log.Printf("pg: discarding bad listener connection: %s", err)
	}
	return l.closeConn(err)
}

func (l *Listener) closeConn(err error) (retErr error) {
	l.mx.Lock()
	if l._cn != nil {
		retErr = l.db.pool.Remove(l._cn, err)
		l._cn = nil
	}
	l.mx.Unlock()
	return retErr
}

func (l *Listener) Close() error {
	l.mx.Lock()
	closed := l.closed
	l.closed = true
	l.mx.Unlock()
	if closed {
		return errListenerClosed
	}
	return l.closeConn(errListenerClosed)
}
