package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	stateDefault = 0
	stateInited  = 1
	stateClosed  = 2
)

type SingleConnPool struct {
	pool      Pooler
	settingUp sync.Once
	closing   sync.Once
	setupErr  error

	state uint32 // atomic
	ch    chan *Conn

	level      int32  // atomic
	hasBadConn uint32 // atomic
}

var _ Pooler = (*SingleConnPool)(nil)

func NewSingleConnPool(pool Pooler) *SingleConnPool {
	p, ok := pool.(*SingleConnPool)
	if !ok {
		p = &SingleConnPool{
			pool: pool,
			ch:   make(chan *Conn, 1),
		}
	}
	atomic.AddInt32(&p.level, 1)
	return p
}

func (p *SingleConnPool) Clone() *SingleConnPool {
	return NewSingleConnPool(p.pool)
}

func (p *SingleConnPool) SetConn(cn *Conn) {
	if atomic.CompareAndSwapUint32(&p.state, stateDefault, stateInited) {
		p.ch <- cn
		return
	}
	panic("not reached")
}

func (p *SingleConnPool) NewConn(c context.Context) (*Conn, error) {
	return p.pool.NewConn(c)
}

func (p *SingleConnPool) CloseConn(cn *Conn) error {
	return p.pool.CloseConn(cn)
}

func (p *SingleConnPool) Get(c context.Context) (*Conn, error) {
	p.settingUp.Do(func() {
		atomic.StoreUint32(&p.state, stateInited)
		cn, err := p.pool.Get(c)
		if err != nil {
			p.setupErr = err
			return
		}
		p.ch <- cn
	})
	if p.setupErr != nil {
		return nil, p.setupErr
	}

	cn, ok := <-p.ch
	if !ok {
		return nil, ErrClosed
	}
	return cn, nil
}

func (p *SingleConnPool) Put(cn *Conn) {
	defer func() {
		if recover() != nil {
			p.pool.Put(cn)
		}
	}()
	p.ch <- cn
}

func (p *SingleConnPool) Remove(cn *Conn) {
	defer func() {
		if recover() != nil {
			p.pool.Remove(cn)
		}
	}()
	atomic.StoreUint32(&p.hasBadConn, 1)
	p.ch <- cn
}

func (p *SingleConnPool) Len() int {
	switch atomic.LoadUint32(&p.state) {
	case stateDefault:
		return 0
	case stateInited:
		return 1
	case stateClosed:
		return 0
	default:
		panic("not reached")
	}
}

func (p *SingleConnPool) IdleLen() int {
	return len(p.ch)
}

func (p *SingleConnPool) Stats() *Stats {
	return &Stats{}
}

func (p *SingleConnPool) Close() error {
	level := atomic.AddInt32(&p.level, -1)
	if level > 0 {
		return nil
	}

	p.closing.Do(func() {
		// Make sure conn pool cannot be set up after close
		p.settingUp.Do(func() {})

		atomic.StoreUint32(&p.state, stateClosed)
		close(p.ch)
		cn, ok := <-p.ch
		if ok {
			if atomic.LoadUint32(&p.hasBadConn) == 1 {
				p.pool.Remove(cn)
			} else {
				p.pool.Put(cn)
			}
		}
	})

	return nil
}

func (p *SingleConnPool) Reset() error {
	if atomic.LoadUint32(&p.hasBadConn) == 0 {
		return nil
	}

	cn, ok := <-p.ch
	if !ok {
		return fmt.Errorf("pg: SingleConnPool does not have a Conn")
	}
	p.pool.Remove(cn)

	if !atomic.CompareAndSwapUint32(&p.state, stateInited, stateDefault) {
		state := atomic.LoadUint32(&p.state)
		return fmt.Errorf("pg: invalid SingleConnPool state: %d", state)
	}

	return nil
}
