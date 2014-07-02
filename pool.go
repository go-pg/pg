package pg

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/golang/glog"
)

var (
	errRateLimited = errors.New("pg: you open connections too fast")
)

type connPool struct {
	dial func() (*conn, error)
	rl   *rateLimiter

	cond  *sync.Cond
	conns *list.List

	maxSize int

	idleNum            int
	idleTimeout        time.Duration
	idleCheckFrequency time.Duration
	idleCheckTicker    *time.Ticker

	closed bool
}

func newConnPool(opt *Options) *connPool {
	p := &connPool{
		dial: newConnFunc(opt),
		rl:   newRateLimiter(3*time.Second, 2*opt.getPoolSize()),

		cond:  sync.NewCond(&sync.Mutex{}),
		conns: list.New(),

		maxSize: opt.getPoolSize(),

		idleTimeout:        opt.getIdleTimeout(),
		idleCheckFrequency: opt.getIdleCheckFrequency(),
	}
	if p.idleTimeout > 0 && p.idleCheckFrequency > 0 {
		go p.reaper()
	}
	return p
}

func (p *connPool) new() (*conn, error) {
	select {
	case <-p.rl.C:
	default:
		return nil, errRateLimited
	}
	return p.dial()
}

func (p *connPool) Get() (*conn, bool, error) {
	p.cond.L.Lock()

	if p.closed {
		p.cond.L.Unlock()
		return nil, false, errClosed
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
		cn, err := p.new()
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
	if cn.br.Buffered() > 0 {
		b, _ := cn.br.Peek(cn.br.Buffered())
		glog.Errorf("pg: discarding connection that has unread data: %.100q", b)
		return p.Remove(cn)
	}

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

func (p *connPool) remove(cn *conn) error {
	p.conns.Remove(cn.elem)
	if !cn.inUse {
		p.idleNum--
	}
	cn.elem = nil
	return cn.Close()
}

func (p *connPool) Remove(cn *conn) error {
	p.cond.L.Lock()
	if p.closed {
		// Noop, connection is already closed.
		p.cond.L.Unlock()
		return nil
	}
	err := p.remove(cn)
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

	if p.idleCheckTicker != nil {
		p.idleCheckTicker.Stop()
	}

	var retErr error
	for {
		e := p.conns.Front()
		if e == nil {
			break
		}
		if err := p.remove(e.Value.(*conn)); err != nil {
			glog.Errorf("cn.Close failed: %s", err)
			retErr = err
		}
	}

	return retErr
}

func (p *connPool) reaper() {
	p.idleCheckTicker = time.NewTicker(p.idleCheckFrequency)
	for _ = range p.idleCheckTicker.C {
		p.cond.L.Lock()
		p.closeIdle()
		p.cond.L.Unlock()
	}
}

func (p *connPool) closeIdle() {
	for el := p.conns.Front(); el != nil; el = el.Next() {
		cn := el.Value.(*conn)
		if cn.inUse {
			break
		}
		if cn.IsIdle(p.idleTimeout) {
			if err := p.remove(cn); err != nil {
				glog.Errorf("Remove failed: %s", err)
			}
		}
	}
}
