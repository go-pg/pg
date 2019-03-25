package pool

import (
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-pg/pg/internal"
)

var noDeadline = time.Time{}

type Conn struct {
	netConn net.Conn

	buf []byte
	rd  *internal.BufReader
	wb  *WriteBuffer

	Inited    bool
	pooled    bool
	createdAt time.Time
	usedAt    atomic.Value

	ProcessId int32
	SecretKey int32

	_lastId int64
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		buf: makeBuffer(),
		rd:  internal.NewBufReader(netConn),
		wb:  NewWriteBuffer(),

		createdAt: time.Now(),
	}
	cn.SetNetConn(netConn)
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	return cn.usedAt.Load().(time.Time)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	cn.usedAt.Store(tm)
}

func (cn *Conn) RemoteAddr() net.Addr {
	return cn.netConn.RemoteAddr()
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.rd.Reset(netConn)
}

func (cn *Conn) NetConn() net.Conn {
	return cn.netConn
}

func (cn *Conn) NextId() string {
	cn._lastId++
	return strconv.FormatInt(cn._lastId, 10)
}

func (cn *Conn) setReadTimeout(timeout time.Duration) error {
	now := time.Now()
	cn.SetUsedAt(now)
	if timeout > 0 {
		return cn.netConn.SetReadDeadline(now.Add(timeout))
	}
	return cn.netConn.SetReadDeadline(noDeadline)
}

func (cn *Conn) setWriteTimeout(timeout time.Duration) error {
	now := time.Now()
	cn.SetUsedAt(now)
	if timeout > 0 {
		return cn.netConn.SetWriteDeadline(now.Add(timeout))
	}
	return cn.netConn.SetWriteDeadline(noDeadline)
}

func (cn *Conn) WithReader(timeout time.Duration, fn func(rd *internal.BufReader) error) error {
	_ = cn.setReadTimeout(timeout)

	err := fn(cn.rd)

	return err
}

func (cn *Conn) WithWriter(timeout time.Duration, fn func(wb *WriteBuffer) error) error {
	_ = cn.setWriteTimeout(timeout)

	cn.wb.ResetBuffer(cn.buf)

	firstErr := fn(cn.wb)

	_, err := cn.netConn.Write(cn.wb.Bytes)
	cn.buf = cn.wb.Buffer()
	if err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

func makeBuffer() []byte {
	const defaulBufSize = 4096
	return make([]byte, defaulBufSize)
}
