package pool

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

var noDeadline = time.Time{}

type Conn struct {
	netConn net.Conn

	Reader  *bufio.Reader
	readBuf []byte
	Columns [][]byte

	Writer *WriteBuffer

	InitedAt time.Time
	usedAt   atomic.Value

	ProcessId int32
	SecretKey int32

	_lastId int64
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		Reader:  bufio.NewReader(netConn),
		readBuf: make([]byte, 0, 512),

		Writer: NewWriteBuffer(),
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

func (cn *Conn) ReadN(n int) ([]byte, error) {
	if d := n - cap(cn.readBuf); d > 0 {
		cn.readBuf = cn.readBuf[:cap(cn.readBuf)]
		cn.readBuf = append(cn.readBuf, make([]byte, d)...)
	} else {
		cn.readBuf = cn.readBuf[:n]
	}
	_, err := io.ReadFull(cn.Reader, cn.readBuf)
	return cn.readBuf, err
}

func (cn *Conn) FlushWriter() error {
	_, err := cn.netConn.Write(cn.Writer.Bytes)
	cn.Writer.Reset()
	return err
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

func (cn *Conn) CheckHealth() error {
	if cn.Reader.Buffered() != 0 {
		b, _ := cn.Reader.Peek(cn.Reader.Buffered())
		err := fmt.Errorf("connection has unread data:\n%s", hex.Dump(b))
		return err
	}
	return nil
}
