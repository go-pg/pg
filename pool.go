package pg

import (
	"container/list"
	"sync"
	"time"
)

type defaultPool struct {
	dial  func() (*conn, error)
	close func(*conn) error

	cond  *sync.Cond
	conns *list.List

	size, maxSize int
	idleTimeout   time.Duration
}

func newDefaultPool(
	dial func() (*conn, error),
	close func(*conn) error,
	maxSize int, idleTimeout time.Duration,
) *defaultPool {
	return &defaultPool{
		dial:  dial,
		close: close,

		cond:  sync.NewCond(&sync.Mutex{}),
		conns: list.New(),

		maxSize:     maxSize,
		idleTimeout: idleTimeout,
	}
}

func (p *defaultPool) Get() (*conn, bool, error) {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()

	for p.conns.Len() == 0 && p.size >= p.maxSize {
		p.cond.Wait()
	}

	if p.idleTimeout > 0 {
		for e := p.conns.Front(); e != nil; e = e.Next() {
			cn := e.Value.(*conn)
			if time.Since(cn.LastActivity) > p.idleTimeout {
				p.conns.Remove(e)
			}
		}
	}

	if p.conns.Len() == 0 {
		cn, err := p.dial()
		if err != nil {
			return nil, false, err
		}
		p.size++
		return cn, true, nil
	}

	elem := p.conns.Front()
	p.conns.Remove(elem)
	return elem.Value.(*conn), false, nil
}

func (p *defaultPool) Put(cn *conn) error {
	if p.idleTimeout > 0 {
		cn.LastActivity = time.Now()
	}

	p.cond.L.Lock()
	p.conns.PushFront(cn)
	p.cond.Signal()
	p.cond.L.Unlock()

	return nil
}

func (p *defaultPool) Remove(cn *conn) error {
	var err error
	if cn != nil {
		err = p.close(cn)
	}
	p.cond.L.Lock()
	p.size--
	p.cond.Signal()
	p.cond.L.Unlock()
	return err
}

func (p *defaultPool) Len() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return p.conns.Len()
}

func (p *defaultPool) Size() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return p.size
}

func (p *defaultPool) Close() error {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()

	for e := p.conns.Front(); e != nil; e = e.Next() {
		if err := p.close(e.Value.(*conn)); err != nil {
			return err
		}
	}
	p.conns.Init()
	p.size = 0

	return nil
}
