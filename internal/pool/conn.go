package pool

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

var noDeadline = time.Time{}

type Conn struct {
	netConn net.Conn

	Reader  *BufioReader
	Columns [][]byte

	wb               *WriteBuffer
	concurrentWrites bool

	InitedAt time.Time
	usedAt   atomic.Value

	ProcessId int32
	SecretKey int32

	_lastId int64
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		Reader: NewBufioReader(netConn),
		wb:     NewWriteBuffer(),
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
	cn.Reader.Reset(netConn)
}

func (cn *Conn) NetConn() net.Conn {
	return cn.netConn
}

func (cn *Conn) NextId() string {
	cn._lastId++
	return strconv.FormatInt(cn._lastId, 10)
}

func (cn *Conn) SetTimeout(rt, wt time.Duration) {
	now := time.Now()
	cn.SetUsedAt(now)
	if rt > 0 {
		_ = cn.netConn.SetReadDeadline(now.Add(rt))
	} else {
		_ = cn.netConn.SetReadDeadline(noDeadline)
	}
	if wt > 0 {
		_ = cn.netConn.SetWriteDeadline(now.Add(wt))
	} else {
		_ = cn.netConn.SetWriteDeadline(noDeadline)
	}
}

func (cn *Conn) EnableConcurrentWrites() {
	cn.concurrentWrites = true
	cn.wb.Bytes = make([]byte, defaultBufSize)
}

func (cn *Conn) PrepareWriteBuffer() *WriteBuffer {
	if !cn.concurrentWrites {
		cn.wb.Bytes = cn.Reader.Buffer()
	}
	cn.wb.Reset()
	return cn.wb
}

func (cn *Conn) FlushWriteBuffer(buf *WriteBuffer) error {
	_, err := cn.netConn.Write(buf.Bytes)
	if !cn.concurrentWrites {
		cn.Reader.ResetBuffer(cn.wb.Bytes[:cap(cn.wb.Bytes)])
	}
	return err
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

func (cn *Conn) CheckHealth() error {
	if buf := cn.Reader.Bytes(); len(buf) > 0 {
		err := fmt.Errorf("connection has unread data:\n%s", hex.Dump(buf))
		return err
	}
	return nil
}
