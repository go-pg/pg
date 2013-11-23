package pg

import (
	"fmt"
	"time"
)

type Listener struct {
	db  *DB
	_cn *conn
}

func (l *Listener) conn(readTimeout time.Duration) *conn {
	l._cn.SetReadTimeout(readTimeout)
	l._cn.SetWriteTimeout(l.db.opt.WriteTimeout)
	return l._cn
}

func (l *Listener) Listen(channels ...string) error {
	cn := l.conn(l.db.opt.ReadTimeout)
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
	cn := l.conn(readTimeout)

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
	return l.db.pool.Remove(l._cn)
}
