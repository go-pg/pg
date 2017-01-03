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
	NetConn net.Conn

	Buf []byte // read/write buffer

	Rd      *bufio.Reader
	Columns [][]byte

	Wr *WriteBuffer

	Inited bool
	UsedAt time.Time

	ProcessId int32
	SecretKey int32

	_lastId int64
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		Buf:    make([]byte, 0, 8192),
		UsedAt: time.Now(),
	}
	cn.SetNetConn(netConn)
	return cn
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.NetConn = netConn
	cn.Rd = bufio.NewReader(cn.NetConn)
	cn.Wr = NewWriteBuffer(cn.NetConn, cn.Buf)
}

func (cn *Conn) IsStale(timeout time.Duration) bool {
	return timeout > 0 && time.Since(cn.UsedAt) >= timeout
}

func (cn *Conn) NextId() string {
	cn._lastId++
	return strconv.FormatInt(cn._lastId, 10)
}

func (cn *Conn) SetReadWriteTimeout(rt, wt time.Duration) {
	cn.UsedAt = time.Now()
	if rt > 0 {
		cn.NetConn.SetReadDeadline(cn.UsedAt.Add(rt))
	} else {
		cn.NetConn.SetReadDeadline(noDeadline)
	}
	if wt > 0 {
		cn.NetConn.SetWriteDeadline(cn.UsedAt.Add(wt))
	} else {
		cn.NetConn.SetWriteDeadline(noDeadline)
	}
}

func (cn *Conn) ReadN(n int) ([]byte, error) {
	if d := n - cap(cn.Buf); d > 0 {
		cn.Buf = cn.Buf[:cap(cn.Buf)]
		cn.Buf = append(cn.Buf, make([]byte, d)...)
	} else {
		cn.Buf = cn.Buf[:n]
	}
	_, err := io.ReadFull(cn.Rd, cn.Buf)
	return cn.Buf, err
}

func (cn *Conn) Close() error {
	return cn.NetConn.Close()
}

func (cn *Conn) CheckHealth() error {
	if cn.Rd.Buffered() != 0 {
		b, _ := cn.Rd.Peek(cn.Rd.Buffered())
		err := fmt.Errorf("connection has unread data:\n%s", hex.Dump(b))
		return err
	}

	return nil
}
