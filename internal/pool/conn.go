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

	rd *internal.BufReader
	wb *WriteBuffer

	ProcessID int32
	SecretKey int32
	lastID    int64

	pooled    bool
	Inited    bool
	createdAt time.Time
	usedAt    int64 // atomic
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		rd: internal.NewBufReader(netConn),
		wb: NewWriteBuffer(),

		createdAt: time.Now(),
	}
	cn.SetNetConn(netConn)
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
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

func (cn *Conn) NextID() string {
	cn.lastID++
	return strconv.FormatInt(cn.lastID, 10)
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
	firstErr := fn(cn.wb)

	buf := cn.wb.Flush()
	_, err := cn.netConn.Write(buf)
	if err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}
