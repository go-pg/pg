package pg

import (
	"fmt"
)

type Notification struct {
	Channel string
	Payload string
	Err     error
}

type Listener struct {
	pool *defaultPool
	cn   *conn

	Chan chan *Notification
}

func newListener(pool *defaultPool, cn *conn) *Listener {
	l := &Listener{
		pool: pool,
		cn:   cn,
		Chan: make(chan *Notification),
	}
	go l.listen()
	return l
}

func (l *Listener) Listen(channel string) error {
	return writeQueryMsg(l.cn, "LISTEN ?", F(channel))
}

func (l *Listener) Close() error {
	return l.pool.Remove(l.cn)
}

func (l *Listener) listen() {
	for {
		notif := &Notification{}

		c, msgLen, err := l.cn.ReadMsgType()
		_ = msgLen
		if err != nil {
			notif.Err = err
			l.Chan <- notif
			break
		}

		switch c {
		case commandCompleteMsg:
			_, notif.Err = l.cn.br.ReadN(msgLen)
			if err != nil {
				l.Chan <- notif
				break
			}
		case readyForQueryMsg:
			_, notif.Err = l.cn.br.ReadN(msgLen)
			if err != nil {
				l.Chan <- notif
				break
			}
		case notificationResponseMsg:
			_, notif.Err = l.cn.ReadInt32()
			if notif.Err != nil {
				l.Chan <- notif
				break
			}
			notif.Channel, notif.Err = l.cn.ReadString()
			if notif.Err != nil {
				l.Chan <- notif
				break
			}
			notif.Payload, notif.Err = l.cn.ReadString()
			if notif.Err != nil {
				l.Chan <- notif
				break
			}
			l.Chan <- notif
		default:
			notif.Err = fmt.Errorf("pg: unexpected message %q", c)
			l.Chan <- notif
			break
		}
	}

	close(l.Chan)
}
