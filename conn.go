package pg

import (
	"container/list"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"gopkg.in/bufio.v1"
)

var noDeadline = time.Time{}

func dial(opt *Options) (net.Conn, error) {
	return net.DialTimeout(opt.getNetwork(), opt.getAddr(), opt.getDialTimeout())
}

type conn struct {
	opt *Options
	cn  net.Conn
	rd  *bufio.Reader // read buffer
	buf *buffer       // write buffer

	inUse  bool
	usedAt time.Time

	processId int32
	secretKey int32

	_id int64

	elem *list.Element
}

func newConnDialer(opt *Options) func() (*conn, error) {
	return func() (*conn, error) {
		netcn, err := dial(opt)
		if err != nil {
			return nil, err
		}
		cn := &conn{
			opt: opt,
			cn:  netcn,
			buf: newBuffer(),
		}
		cn.rd = bufio.NewReader(cn)
		if err := cn.Startup(); err != nil {
			return nil, err
		}
		if err := setParams(cn, opt.Params); err != nil {
			return nil, err
		}
		return cn, nil
	}
}

func (cn *conn) GenId() string {
	cn._id++
	return strconv.FormatInt(cn._id, 10)
}

func (cn *conn) SetReadTimeout(dur time.Duration) {
	if dur == 0 {
		cn.cn.SetReadDeadline(noDeadline)
	} else {
		cn.cn.SetReadDeadline(time.Now().Add(dur))
	}
}

func (cn *conn) SetWriteTimeout(dur time.Duration) {
	if dur == 0 {
		cn.cn.SetWriteDeadline(noDeadline)
	} else {
		cn.cn.SetWriteDeadline(time.Now().Add(dur))
	}
}

func (cn *conn) Read(b []byte) (int, error) {
	return cn.cn.Read(b)
}

func (cn *conn) Write(b []byte) (int, error) {
	return cn.cn.Write(b)
}

func (cn *conn) Close() error {
	writeTerminateMsg(cn.buf)
	_ = cn.FlushWrite()
	return cn.cn.Close()
}

func (cn *conn) ssl() error {
	writeSSLMsg(cn.buf)
	if err := cn.FlushWrite(); err != nil {
		return err
	}

	b := make([]byte, 1)
	_, err := io.ReadFull(cn.cn, b)
	if err != nil {
		return err
	}
	if b[0] != 'S' {
		return ErrSSLNotSupported
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
	}
	cn.cn = tls.Client(cn.cn, tlsConf)

	return nil
}

func (cn *conn) Startup() error {
	if cn.opt.getSSL() {
		if err := cn.ssl(); err != nil {
			return err
		}
	}

	writeStartupMsg(cn.buf, cn.opt.getUser(), cn.opt.getDatabase())
	if err := cn.FlushWrite(); err != nil {
		return err
	}

	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return err
		}
		switch c {
		case backendKeyDataMsg:
			processId, err := cn.ReadInt32()
			if err != nil {
				return err
			}
			secretKey, err := cn.ReadInt32()
			if err != nil {
				return err
			}
			cn.processId = processId
			cn.secretKey = secretKey
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return err
			}
		case authenticationOKMsg:
			if err := cn.auth(); err != nil {
				return err
			}
		case readyForQueryMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return err
			}
			return nil
		case errorResponseMsg:
			e, err := cn.ReadError()
			if err != nil {
				return err
			}
			return e
		default:
			return fmt.Errorf("pg: unknown startup message response: %q", c)
		}
	}
}

func (cn *conn) auth() error {
	num, err := cn.ReadInt32()
	if err != nil {
		return err
	}
	switch num {
	case 0:
		return nil
	case 3:
		writePasswordMsg(cn.buf, cn.opt.getPassword())
		if err := cn.FlushWrite(); err != nil {
			return err
		}

		c, _, err := cn.ReadMsgType()
		if err != nil {
			return err
		}
		switch c {
		case authenticationOKMsg:
			num, err := cn.ReadInt32()
			if err != nil {
				return err
			}
			if num != 0 {
				return fmt.Errorf("pg: unexpected authentication code: %d", num)
			}
			return nil
		case errorResponseMsg:
			e, err := cn.ReadError()
			if err != nil {
				return err
			}
			return e
		default:
			return fmt.Errorf("pg: unknown password message response: %q", c)
		}
	case 5:
		b, err := cn.ReadN(4)
		if err != nil {
			return err
		}

		secret := "md5" + md5s(md5s(cn.opt.getPassword()+cn.opt.getUser())+string(b))
		writePasswordMsg(cn.buf, secret)
		if err := cn.FlushWrite(); err != nil {
			return err
		}

		c, _, err := cn.ReadMsgType()
		if err != nil {
			return err
		}
		switch c {
		case authenticationOKMsg:
			num, err := cn.ReadInt32()
			if err != nil {
				return err
			}
			if num != 0 {
				return fmt.Errorf("pg: unexpected authentication code: %d", num)
			}
			return nil
		case errorResponseMsg:
			e, err := cn.ReadError()
			if err != nil {
				return err
			}
			return e
		default:
			return fmt.Errorf("pg: unknown password message response: %q", c)
		}
	default:
		return fmt.Errorf("pg: unknown authentication message response: %d", num)
	}
}

func (cn *conn) ReadN(n int) ([]byte, error) {
	b, err := cn.rd.ReadN(n)
	if err == bufio.ErrBufferFull {
		tmp := make([]byte, n)
		r := copy(tmp, b)
		b = tmp

		for {
			nn, err := cn.rd.Read(b[r:])
			r += nn
			if r >= n {
				// Ignore error if we read enough.
				break
			}
			if err != nil {
				return nil, err
			}
		}
	} else if err != nil {
		return nil, err
	}
	return b, nil
}

func (cn *conn) ReadInt16() (int16, error) {
	b, err := cn.ReadN(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func (cn *conn) ReadInt32() (int32, error) {
	b, err := cn.ReadN(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func (cn *conn) ReadMsgType() (msgType, int, error) {
	c, err := cn.rd.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	l, err := cn.ReadInt32()
	if err != nil {
		return 0, 0, err
	}
	return msgType(c), int(l) - 4, nil
}

func (cn *conn) ReadString() (string, error) {
	s, err := cn.rd.ReadString(0)
	if err != nil {
		return "", err
	}
	return s[:len(s)-1], nil
}

func (cn *conn) ReadError() (error, error) {
	e := &pgError{make(map[byte]string)}
	for {
		c, err := cn.rd.ReadByte()
		if err != nil {
			return nil, err
		}
		if c == 0 {
			break
		}
		s, err := cn.ReadString()
		if err != nil {
			return nil, err
		}
		e.c[c] = s
	}

	switch e.Field('C') {
	case "23000", "23001", "23502", "23503", "23505", "23514", "23P01":
		return &IntegrityError{pgError: e}, nil
	}
	return e, nil
}

func (cn *conn) FlushWrite() error {
	b := cn.buf.Flush()
	n, err := cn.cn.Write(b)
	if n == len(b) {
		return nil
	}
	return err
}
