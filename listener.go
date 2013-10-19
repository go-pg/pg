package pg

import (
	"fmt"
)

type Listener struct {
	pool *defaultPool
	cn   *conn
}

func (l *Listener) Listen(channels ...string) error {
	for _, name := range channels {
		if err := writeQueryMsg(l.cn.buf, "LISTEN ?", F(name)); err != nil {
			return err
		}
	}
	return l.cn.Flush()
}

func (l *Listener) Close() error {
	return l.pool.Remove(l.cn)
}

func (l *Listener) Receive() (string, string, error) {
	for {
		c, msgLen, err := l.cn.ReadMsgType()
		if err != nil {
			return "", "", err
		}

		switch c {
		case commandCompleteMsg:
			_, err := l.cn.br.ReadN(msgLen)
			if err != nil {
				return "", "", err
			}
		case readyForQueryMsg:
			_, err := l.cn.br.ReadN(msgLen)
			if err != nil {
				return "", "", err
			}
		case errorResponseMsg:
			e, err := l.cn.ReadError()
			if err != nil {
				return "", "", err
			}
			return "", "", e
		case notificationResponseMsg:
			_, err := l.cn.ReadInt32()
			if err != nil {
				return "", "", err
			}
			channel, err := l.cn.ReadString()
			if err != nil {
				return "", "", err
			}
			payload, err := l.cn.ReadString()
			if err != nil {
				return "", "", err
			}
			return channel, payload, nil
		default:
			return "", "", fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}
