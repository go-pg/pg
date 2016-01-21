package pg

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/bsm/ratelimit.v1"
)

var (
	errPoolTimeout = errors.New("pg: connection pool timeout")
)

type pool interface {
	First() *conn
	Get() (*conn, error)
	Put(*conn) error
	Remove(*conn) error
	Len() int
	FreeLen() int
	Close() error
}

type connList struct {
	cns  []*conn
	mx   sync.Mutex
	len  int32 // atomic
	size int32
}

func newConnList(size int) *connList {
	return &connList{
		cns:  make([]*conn, 0, size),
		size: int32(size),
	}
}

func (l *connList) Len() int {
	return int(atomic.LoadInt32(&l.len))
}

// Reserve reserves place in the list and returns true on success. The
// caller must add or remove connection if place was reserved.
func (l *connList) Reserve() bool {
	len := atomic.AddInt32(&l.len, 1)
	reserved := len <= l.size
	if !reserved {
		atomic.AddInt32(&l.len, -1)
	}
	return reserved
}

// Add adds connection to the list. The caller must reserve place first.
func (l *connList) Add(cn *conn) {
	l.mx.Lock()
	l.cns = append(l.cns, cn)
	l.mx.Unlock()
}

// Remove closes connection and removes it from the list.
func (l *connList) Remove(cn *conn) error {
	defer l.mx.Unlock()
	l.mx.Lock()

	if cn == nil {
		atomic.AddInt32(&l.len, -1)
		return nil
	}

	for i, c := range l.cns {
		if c == cn {
			l.cns = append(l.cns[:i], l.cns[i+1:]...)
			atomic.AddInt32(&l.len, -1)
			return cn.Close()
		}
	}

	if l.closed() {
		return nil
	}
	panic("conn not found in the list")
}

func (l *connList) Replace(cn, newcn *conn) error {
	defer l.mx.Unlock()
	l.mx.Lock()

	for i, c := range l.cns {
		if c == cn {
			l.cns[i] = newcn
			return cn.Close()
		}
	}

	if l.closed() {
		return newcn.Close()
	}
	panic("conn not found in the list")
}

func (l *connList) Close() (retErr error) {
	l.mx.Lock()
	for _, c := range l.cns {
		if err := c.Close(); err != nil {
			retErr = err
		}
	}
	l.cns = nil
	atomic.StoreInt32(&l.len, 0)
	l.mx.Unlock()
	return retErr
}

func (l *connList) closed() bool {
	return l.cns == nil
}

type connPool struct {
	dialer func() (*conn, error)

	rl        *ratelimit.RateLimiter
	opt       *Options
	conns     *connList
	freeConns chan *conn

	_closed int32

	lastErr atomic.Value
}

func newConnPool(opt *Options) *connPool {
	p := &connPool{
		dialer: newConnDialer(opt),

		rl:        ratelimit.New(3*opt.getPoolSize(), time.Second),
		opt:       opt,
		conns:     newConnList(opt.getPoolSize()),
		freeConns: make(chan *conn, opt.getPoolSize()),
	}
	if p.opt.getIdleTimeout() > 0 && p.opt.IdleCheckFrequency > 0 {
		go p.reaper()
	}
	return p
}

func (p *connPool) closed() bool {
	return atomic.LoadInt32(&p._closed) == 1
}

func (p *connPool) isIdle(cn *conn) bool {
	return p.opt.getIdleTimeout() > 0 && time.Since(cn.usedAt) > p.opt.getIdleTimeout()
}

// First returns first non-idle connection from the pool or nil if
// there are no connections.
func (p *connPool) First() *conn {
	for {
		select {
		case cn := <-p.freeConns:
			if p.isIdle(cn) {
				var err error
				cn, err = p.replace(cn)
				if err != nil {
					log.Printf("pg: replace failed: %s", err)
					continue
				}
			}
			return cn
		default:
			return nil
		}
	}
	panic("not reached")
}

// wait waits for free non-idle connection. It returns nil on timeout.
func (p *connPool) wait() *conn {
	deadline := time.After(p.opt.getPoolTimeout())
	for {
		select {
		case cn := <-p.freeConns:
			if p.isIdle(cn) {
				var err error
				cn, err = p.replace(cn)
				if err != nil {
					log.Printf("pg: replace failed: %s", err)
					continue
				}
			}
			return cn
		case <-deadline:
			return nil
		}
	}
	panic("not reached")
}

// Establish a new connection
func (p *connPool) new() (*conn, error) {
	if p.closed() {
		return nil, errClosed
	}

	if p.rl.Limit() {
		err := fmt.Errorf(
			"pg: you open connections too fast (last_error=%q)",
			p.loadLastErr(),
		)
		return nil, err
	}

	cn, err := p.dialer()
	if err != nil {
		p.storeLastErr(err.Error())
		return nil, err
	}

	return cn, nil
}

// Get returns existed connection from the pool or creates a new one.
func (p *connPool) Get() (cn *conn, isNew bool, err error) {
	if p.closed() {
		err = errClosed
		return
	}

	// Fetch first non-idle connection, if available.
	if cn = p.First(); cn != nil {
		return
	}

	// Try to create a new one.
	if p.conns.Reserve() {
		cn, err = p.new()
		if err != nil {
			p.conns.Remove(nil)
			return
		}
		p.conns.Add(cn)
		isNew = true
		return
	}

	// Otherwise, wait for the available connection.
	if cn = p.wait(); cn != nil {
		return
	}

	err = errPoolTimeout
	return
}

func (p *connPool) Put(cn *conn) error {
	if cn.rd.Buffered() != 0 {
		b, _ := cn.rd.Peek(cn.rd.Buffered())
		err := fmt.Errorf("pg: connection has unread data: %q", b)
		log.Print(err)
		return p.Remove(cn, err)
	}
	if p.opt.getIdleTimeout() > 0 {
		cn.usedAt = time.Now()
	}
	p.freeConns <- cn
	return nil
}

func (p *connPool) replace(cn *conn) (*conn, error) {
	newcn, err := p.new()
	if err != nil {
		_ = p.conns.Remove(cn)
		return nil, err
	}
	_ = p.conns.Replace(cn, newcn)
	return newcn, nil
}

func (p *connPool) Remove(cn *conn, reason error) error {
	p.storeLastErr(reason.Error())

	// Replace existing connection with new one and unblock waiter.
	newcn, err := p.replace(cn)
	if err != nil {
		return err
	}
	p.freeConns <- newcn
	return nil
}

// Len returns total number of connections.
func (p *connPool) Len() int {
	return p.conns.Len()
}

// FreeLen returns number of free connections.
func (p *connPool) FreeLen() int {
	return len(p.freeConns)
}

func (p *connPool) Close() (retErr error) {
	if !atomic.CompareAndSwapInt32(&p._closed, 0, 1) {
		return errClosed
	}
	// Wait for app to free connections, but don't close them immediately.
	for i := 0; i < p.Len(); i++ {
		if cn := p.wait(); cn == nil {
			break
		}
	}
	// Close all connections.
	if err := p.conns.Close(); err != nil && retErr == nil {
		retErr = err
	}
	return retErr
}

func (p *connPool) reaper() {
	ticker := time.NewTicker(p.opt.IdleCheckFrequency)
	defer ticker.Stop()

	for _ = range ticker.C {
		if p.closed() {
			break
		}

		// pool.First removes idle connections from the pool and
		// returns first non-idle connection. So just put returned
		// connection back.
		if cn := p.First(); cn != nil {
			p.Put(cn)
		}
	}
}

func (p *connPool) storeLastErr(err string) {
	p.lastErr.Store(err)
}

func (p *connPool) loadLastErr() string {
	if v := p.lastErr.Load(); v != nil {
		return v.(string)
	}
	return ""
}
