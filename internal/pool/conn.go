package pool

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-pg/pg/v9/internal"
)

var noDeadline = time.Time{}

type Conn struct {
	netConn net.Conn

	rd *internal.BufReader

	ProcessID int32
	SecretKey int32
	lastID    int64

	createdAt time.Time
	usedAt    uint32 // atomic
	pooled    bool
	Inited    bool
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		rd: internal.NewBufReader(netConn),

		createdAt: time.Now(),
	}
	cn.SetNetConn(netConn)
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadUint32(&cn.usedAt)
	return time.Unix(int64(unix), 0)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreUint32(&cn.usedAt, uint32(tm.Unix()))
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
	ctx context.Context, timeout time.Duration, fn func(rd *internal.BufReader) error,
) error {
	err := cn.netConn.SetReadDeadline(cn.deadline(ctx, timeout))
	if err != nil {
		return err
	}
	return fn(cn.rd)
}

func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wb *WriteBuffer) error,
) error {
	err := cn.netConn.SetWriteDeadline(cn.deadline(ctx, timeout))
	if err != nil {
		return err
	}

	wb := getWriteBuffer()
	defer putWriteBuffer(wb)

	wb.Reset()
	err = fn(wb)
	if err != nil {
		return err
	}

	_, err = cn.netConn.Write(wb.Bytes)
	return err
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
