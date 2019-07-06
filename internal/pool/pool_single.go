package pool

import (
	"context"
	"sync"
	"sync/atomic"
)

type SingleConnPool struct {
	pool Pooler

	mu sync.Mutex
	cn *Conn

	level      int32  // atomic
	hasBadConn uint32 // atomic
}

var _ Pooler = (*SingleConnPool)(nil)

func NewSingleConnPool(pool Pooler) *SingleConnPool {
	p, ok := pool.(*SingleConnPool)
	if !ok {
		p = &SingleConnPool{
			pool: pool,
		}
	}
	atomic.AddInt32(&p.level, 1)
	return p
}

func (p *SingleConnPool) Clone() *SingleConnPool {
	return NewSingleConnPool(p.pool)
}

func (p *SingleConnPool) SetConn(cn *Conn) {
	p.mu.Lock()
	if p.cn != nil {
		panic("p.cn != nil")
	}
	p.cn = cn
	p.mu.Unlock()
}

func (p *SingleConnPool) NewConn(c context.Context) (*Conn, error) {
	return p.pool.NewConn(c)
}

func (p *SingleConnPool) CloseConn(cn *Conn) error {
	return p.pool.CloseConn(cn)
}

func (p *SingleConnPool) Get(c context.Context) (*Conn, error) {
	p.mu.Lock()
	cn, err := p.get(c)
	p.mu.Unlock()
	return cn, err
}

func (p *SingleConnPool) get(c context.Context) (*Conn, error) {
	if atomic.LoadInt32(&p.level) == 0 {
		return nil, ErrClosed
	}
	if p.cn != nil {
		return p.cn, nil
	}

	cn, err := p.pool.Get(c)
	if err != nil {
		return nil, err
	}

	p.cn = cn
	return cn, nil
}

func (p *SingleConnPool) Put(cn *Conn) {}

func (p *SingleConnPool) Remove(cn *Conn) {
	atomic.StoreUint32(&p.hasBadConn, 1)
}

func (p *SingleConnPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cn != nil {
		return 1
	}
	return 0
}

func (p *SingleConnPool) IdleLen() int {
	return 0
}

func (p *SingleConnPool) Stats() *Stats {
	return &Stats{}
}

func (p *SingleConnPool) Close() error {
	level := atomic.AddInt32(&p.level, -1)
	if level > 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cn != nil {
		if atomic.LoadUint32(&p.hasBadConn) == 1 {
			p.pool.Remove(p.cn)
		} else {
			p.pool.Put(p.cn)
		}
		p.cn = nil
	}

	return nil
}

func (p *SingleConnPool) Reset() error {
	if atomic.LoadUint32(&p.hasBadConn) == 0 {
		return nil
	}

	p.mu.Lock()
	if p.cn != nil {
		p.pool.Remove(p.cn)
		p.cn = nil
	}
	p.mu.Unlock()

	return nil
}
