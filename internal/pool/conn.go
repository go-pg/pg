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
	Rd      *bufio.Reader // read buffer
	Wr      *Buffer       // write buffer

	Buf     []byte   // reusable
	Columns [][]byte // reusable

	Inited bool
	UsedAt time.Time

	ProcessId int32
	SecretKey int32

	_id int64
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		NetConn: netConn,
		Buf:     make([]byte, 0, 8192),

		UsedAt: time.Now(),
	}
	cn.Rd = bufio.NewReader(cn)
	cn.Wr = NewBuffer(cn, cn.Buf)
	return cn
}

func (cn *Conn) IsStale(timeout time.Duration) bool {
	return timeout > 0 && time.Since(cn.UsedAt) > timeout
}

func (cn *Conn) NextId() string {
	cn._id++
	return strconv.FormatInt(cn._id, 10)
}

func (cn *Conn) SetReadTimeout(dur time.Duration) {
	cn.UsedAt = time.Now()
	if dur == 0 {
		cn.NetConn.SetReadDeadline(noDeadline)
	} else {
		cn.NetConn.SetReadDeadline(cn.UsedAt.Add(dur))
	}
}

func (cn *Conn) SetWriteTimeout(dur time.Duration) {
	cn.UsedAt = time.Now()
	if dur == 0 {
		cn.NetConn.SetWriteDeadline(noDeadline)
	} else {
		cn.NetConn.SetWriteDeadline(cn.UsedAt.Add(dur))
	}
}

func (cn *Conn) Read(b []byte) (int, error) {
	return cn.NetConn.Read(b)
}

func (cn *Conn) Write(b []byte) (int, error) {
	return cn.NetConn.Write(b)
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
