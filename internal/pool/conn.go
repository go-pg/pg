package pool

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

var noDeadline = time.Time{}

type Conn struct {
	netConn net.Conn

	buf     []byte // read buffer
	Rd      *bufio.Reader
	Columns [][]byte

	Wr *WriteBuffer

	InitedAt time.Time
	UsedAt   time.Time

	ProcessId int32
	SecretKey int32

	_lastId int64
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		buf:    make([]byte, 0, 512),
		Rd:     bufio.NewReader(netConn),
		Wr:     NewWriteBuffer(),
		UsedAt: time.Now(),
	}
	cn.SetNetConn(netConn)
	return cn
}

func (cn *Conn) RemoteAddr() net.Addr {
	return cn.netConn.RemoteAddr()
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.Rd.Reset(netConn)
}

func (cn *Conn) NetConn() net.Conn {
	return cn.netConn
}

func (cn *Conn) NextId() string {
	cn._lastId++
	return strconv.FormatInt(cn._lastId, 10)
}

func (cn *Conn) SetReadWriteTimeout(rt, wt time.Duration) {
	cn.UsedAt = time.Now()
	if rt > 0 {
		cn.netConn.SetReadDeadline(cn.UsedAt.Add(rt))
	} else {
		cn.netConn.SetReadDeadline(noDeadline)
	}
	if wt > 0 {
		cn.netConn.SetWriteDeadline(cn.UsedAt.Add(wt))
	} else {
		cn.netConn.SetWriteDeadline(noDeadline)
	}
}

func (cn *Conn) ReadN(n int) ([]byte, error) {
	if d := n - cap(cn.buf); d > 0 {
		cn.buf = cn.buf[:cap(cn.buf)]
		cn.buf = append(cn.buf, make([]byte, d)...)
	} else {
		cn.buf = cn.buf[:n]
	}
	_, err := io.ReadFull(cn.Rd, cn.buf)
	return cn.buf, err
}

func (cn *Conn) FlushWriter() error {
	_, err := cn.netConn.Write(cn.Wr.Bytes)
	cn.Wr.Reset()
	return err
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

func (cn *Conn) CheckHealth() error {
	if cn.Rd.Buffered() != 0 {
		b, _ := cn.Rd.Peek(cn.Rd.Buffered())
		err := fmt.Errorf("connection has unread data:\n%s", hex.Dump(b))
		return err
	}
	return nil
}
