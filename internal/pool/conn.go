package pool

import (
	"context"
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

func (cn *Conn) WithReader(
	c context.Context, timeout time.Duration, fn func(rd *internal.BufReader) error,
) error {
	_ = cn.netConn.SetReadDeadline(cn.deadline(c, timeout))
	return fn(cn.rd)
}

func (cn *Conn) WithWriter(
	c context.Context, timeout time.Duration, fn func(wb *WriteBuffer) error,
) error {
	_ = cn.netConn.SetWriteDeadline(cn.deadline(c, timeout))
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

func (cn *Conn) deadline(ctx context.Context, timeout time.Duration) time.Time {
	tm := time.Now()
	cn.SetUsedAt(tm)

	if timeout > 0 {
		tm = tm.Add(timeout)
	}

	if ctx != nil {
		deadline, ok := ctx.Deadline()
		if ok {
			if timeout == 0 {
				return deadline
			}
			if deadline.Before(tm) {
				return deadline
			}
			return tm
		}
	}

	if timeout > 0 {
		return tm
	}

	return noDeadline
}
