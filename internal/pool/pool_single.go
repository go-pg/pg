package pool

import (
	"fmt"
	"sync/atomic"
)

const (
	stateDefault = 0
	stateInited  = 1
	stateClosed  = 2
)

type SingleConnPool struct {
	pool Pooler

	state uint32 // atomic
	ch    chan *Conn

	opened int32 // atomic
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
	atomic.AddInt32(&p.opened, 1)
	return p
}

func (p *SingleConnPool) SetConn(cn *Conn) {
	if atomic.CompareAndSwapUint32(&p.state, stateDefault, stateInited) {
		p.ch <- cn
		return
	}
	panic("not reached")
}

func (p *SingleConnPool) NewConn() (*Conn, error) {
	panic("not implemented")
}

func (p *SingleConnPool) CloseConn(*Conn) error {
	panic("not implemented")
}

func (p *SingleConnPool) Get() (*Conn, error) {
	for i := 0; i < 1e6; i++ {
		switch atomic.LoadUint32(&p.state) {
		case stateDefault:
			if atomic.CompareAndSwapUint32(&p.state, stateDefault, stateInited) {
				return p.pool.Get()
			}
		case stateInited:
			cn, ok := <-p.ch
			if !ok {
				return nil, ErrClosed
			}
			return cn, nil
		case stateClosed:
			return nil, ErrClosed
		default:
			panic("not reached")
		}
	}
	return nil, fmt.Errorf("pg: SingleConnPool.Get: infinite loop")
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
	opened := atomic.AddInt32(&p.opened, -1)
	if opened > 0 {
		return nil
	}

	for i := 0; i < 1e6; i++ {
		state := atomic.LoadUint32(&p.state)
		if state == stateClosed {
			return nil
		}
		if atomic.CompareAndSwapUint32(&p.state, state, stateClosed) {
			close(p.ch)
			cn, ok := <-p.ch
			if ok {
				p.pool.Put(cn)
			}
			return nil
		}
	}

	return fmt.Errorf("pg: SingleConnPool.Close: infinite loop")
}
