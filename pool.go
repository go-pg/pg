package pg

import (
	"container/list"
	"sync"
	"time"

	"github.com/golang/glog"
)

type connPool struct {
	New func() (*conn, error)

	cond  *sync.Cond
	conns *list.List

	idleNum     int
	maxSize     int
	idleTimeout time.Duration

	closed bool
}

func newConnPool(
	dial func() (*conn, error),
	maxSize int, idleTimeout time.Duration,
) *connPool {
	return &connPool{
		New: dial,

		cond:  sync.NewCond(&sync.Mutex{}),
		conns: list.New(),

		maxSize:     maxSize,
		idleTimeout: idleTimeout,
	}
}

func (p *connPool) Get() (*conn, bool, error) {
	p.cond.L.Lock()

	if p.closed {
		p.cond.L.Unlock()
		return nil, false, errClosed
	}

	if p.idleTimeout > 0 {
		for el := p.conns.Front(); el != nil; el = el.Next() {
			cn := el.Value.(*conn)
			if cn.inUse {
				break
			}
			if time.Since(cn.usedAt) > p.idleTimeout {
				if err := p.remove(cn); err != nil {
					glog.Errorf("Remove failed: %s", err)
				}
			}
		}
	}

	for p.conns.Len() >= p.maxSize && p.idleNum == 0 {
		p.cond.Wait()
	}

	if p.idleNum > 0 {
		elem := p.conns.Front()
		cn := elem.Value.(*conn)
		if cn.inUse {
			panic("pool: precondition failed")
		}
		cn.inUse = true
		p.conns.MoveToBack(elem)
		p.idleNum--

		p.cond.L.Unlock()
		return cn, false, nil
	}

	if p.conns.Len() < p.maxSize {
		cn, err := p.New()
		if err != nil {
			p.cond.L.Unlock()
			return nil, false, err
		}

		cn.inUse = true
		cn.elem = p.conns.PushBack(cn)

		p.cond.L.Unlock()
		return cn, true, nil
	}

	panic("not reached")
}

func (p *connPool) Put(cn *conn) error {
	cn.buf.Reset()
	if p.idleTimeout > 0 {
		cn.usedAt = time.Now()
	}

	p.cond.L.Lock()
	if p.closed {
		p.cond.L.Unlock()
		return errClosed
	}
	cn.inUse = false
	p.conns.MoveToFront(cn.elem)
	p.idleNum++
	p.cond.Signal()
	p.cond.L.Unlock()
	return nil
}

func (p *connPool) remove(cn *conn) (err error) {
	if cn != nil {
		err = cn.Close()
	}
	p.conns.Remove(cn.elem)
	cn.elem = nil
	return err
}

func (p *connPool) Remove(cn *conn) (err error) {
	p.cond.L.Lock()
	if p.closed {
		// Noop, connection is already closed.
		p.cond.L.Unlock()
		return nil
	}
	err = p.remove(cn)
	p.cond.Signal()
	p.cond.L.Unlock()
	return err
}

// Returns number of idle connections.
func (p *connPool) Len() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return p.idleNum
}

// Returns number of connections in the pool.
func (p *connPool) Size() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return p.conns.Len()
}

func (p *connPool) Close() error {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()

	if p.closed {
		return errClosed
	}
	p.closed = true
	var retErr error
	for e := p.conns.Front(); e != nil; e = e.Next() {
		cn := e.Value.(*conn)
		if err := cn.Close(); err != nil {
			glog.Errorf("cn.Close failed: %s", err)
			retErr = err
		}
		cn.elem = nil
	}
	p.conns = nil
	return retErr
}
