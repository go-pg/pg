package pg

import (
	"sync"
)

type defaultPool struct {
	cond  *sync.Cond
	pool  []interface{}
	new   func() (interface{}, error)
	close func(interface{}) error

	size, maxSize int
}

func newDefaultPool(
	newf func() (interface{}, error), closef func(interface{}) error, maxSize int,
) *defaultPool {
	return &defaultPool{
		cond:    sync.NewCond(&sync.Mutex{}),
		pool:    make([]interface{}, 0, maxSize),
		new:     newf,
		close:   closef,
		maxSize: maxSize,
	}
}

func (p *defaultPool) Get() (interface{}, bool, error) {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()

	for len(p.pool) == 0 && p.size >= p.maxSize {
		p.cond.Wait()
	}

	if len(p.pool) == 0 {
		res, err := p.new()
		if err != nil {
			return nil, false, err
		}
		p.size++
		return res, true, nil
	}

	last := len(p.pool) - 1
	res := p.pool[last]
	p.pool[last] = nil
	p.pool = p.pool[:last]

	return res, false, nil
}

func (p *defaultPool) Put(res interface{}) error {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	p.pool = append(p.pool, res)
	p.cond.Signal()
	return nil
}

func (p *defaultPool) Remove(res interface{}) error {
	defer func() {
		p.cond.L.Lock()
		p.size--
		p.cond.Signal()
		p.cond.L.Unlock()
	}()
	if res != nil {
		return p.close(res)
	}
	return nil
}

func (p *defaultPool) Size() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return p.size
}

func (p *defaultPool) Available() int {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	return len(p.pool)
}

func (p *defaultPool) Close() error {
	defer p.cond.L.Unlock()
	p.cond.L.Lock()
	for _, res := range p.pool {
		if err := p.close(res); err != nil {
			return err
		}
	}
	p.pool = p.pool[:0]
	p.size = 0
	return nil
}
