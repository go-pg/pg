package pg

import (
	"fmt"
	"time"
)

// Not thread-safe.
type Listener struct {
	db  *DB
	_cn *conn

	closed bool
}

func (l *Listener) conn(readTimeout time.Duration) (*conn, error) {
	if l._cn == nil {
		return nil, errListenerClosed
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
	for _, name := range channels {
		if err := writeQueryMsg(cn.buf, "LISTEN ?", F(name)); err != nil {
			return err
		}
	}
	return cn.Flush()
}

func (l *Listener) Receive() (channel string, payload string, err error) {
	return l.ReceiveTimeout(0)
}

func (l *Listener) ReceiveTimeout(readTimeout time.Duration) (channel, payload string, err error) {
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
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return "", "", err
			}
		case readyForQueryMsg:
			_, err := cn.br.ReadN(msgLen)
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

func (l *Listener) Close() error {
	if l._cn == nil {
		return errListenerClosed
	}
	err := l.db.pool.Remove(l._cn)
	l._cn = nil
	return err
}
