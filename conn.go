package pg

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/vmihailenco/bufio"
)

type conn struct {
	opt *Options
	cn  net.Conn
	br  *bufio.Reader
	buf *buffer

	LastActivity time.Time
}

func dial(opt *Options) (*conn, error) {
	cn, err := net.Dial("tcp", net.JoinHostPort(opt.getHost(), opt.getPort()))
	if err != nil {
		return nil, err
	}
	return &conn{
		opt: opt,
		cn:  cn,
		buf: newBuffer(),
	}, nil
}

func (cn *conn) Close() error {
	return cn.cn.Close()
}

func (cn *conn) ssl() error {
	cn.buf.StartMsg(0)
	cn.buf.WriteInt32(80877103)
	cn.buf.EndMsg()
	if err := cn.Flush(); err != nil {
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
	cn.br = bufio.NewReader(cn.cn)

	return nil
}

func (cn *conn) Startup() error {
	if cn.opt.getSSL() {
		if err := cn.ssl(); err != nil {
			return err
		}
	} else {
		cn.br = bufio.NewReader(cn.cn)
	}

	cn.buf.StartMsg(0)
	cn.buf.WriteInt32(196608)
	cn.buf.WriteString("user")
	cn.buf.WriteString(cn.opt.getUser())
	cn.buf.WriteString("database")
	cn.buf.WriteString(cn.opt.getDatabase())
	cn.buf.WriteString("")
	cn.buf.EndMsg()
	if err := cn.Flush(); err != nil {
		return err
	}

	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return err
		}
		switch c {
		case backendKeyDataMsg, parameterStatusMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return err
			}
		case authenticationOKMsg:
			if err := cn.auth(); err != nil {
				return err
			}
		case readyForQueryMsg:
			_, err := cn.br.ReadN(msgLen)
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
			return fmt.Errorf("pg: uknown response for startup: %q", c)
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
		cn.buf.StartMsg(passwordMessageMsg)
		cn.buf.WriteString(cn.opt.getPassword())
		cn.buf.EndMsg()
		if err := cn.Flush(); err != nil {
			return err
		}

		c, _, err := cn.ReadMsgType()
		if err != nil {
			return err
		}
		if c != authenticationOKMsg {
			return fmt.Errorf("pg: unexpected password response: %q", c)
		}
		num, err := cn.ReadInt32()
		if err != nil {
			return err
		}
		if num != 0 {
			return fmt.Errorf("pg: unexpected authentication response: %q", num)
		}
		return nil
	case 5:
		b, err := cn.br.ReadN(4)
		if err != nil {
			return err
		}
		s := string(b)

		secret := "md5" + md5s(md5s(cn.opt.getPassword()+cn.opt.getUser())+s)
		cn.buf.StartMsg(passwordMessageMsg)
		cn.buf.WriteString(secret)
		cn.buf.EndMsg()
		if err := cn.Flush(); err != nil {
			return err
		}

		c, _, err := cn.ReadMsgType()
		if err != nil {
			return err
		}
		if c != authenticationOKMsg {
			return fmt.Errorf("pg: unexpected password response: %q", c)
		}
		num, err := cn.ReadInt32()
		if err != nil {
			return err
		}
		if num != 0 {
			return fmt.Errorf("pg: unexpected password response: %q", num)
		}
		return nil
	default:
		return fmt.Errorf("pg: unknown authentication response: %d", num)
	}
}

func (cn *conn) ReadInt16() (int16, error) {
	b, err := cn.br.ReadN(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func (cn *conn) ReadInt32() (int32, error) {
	b, err := cn.br.ReadN(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func (cn *conn) ReadMsgType() (msgType, int, error) {
	c, err := cn.br.ReadByte()
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
	s, err := cn.br.ReadString(0)
	if err != nil {
		return "", err
	}
	return s[:len(s)-1], nil
}

func (cn *conn) ReadError() (error, error) {
	e := &pgError{make(map[byte]string)}
	for {
		c, err := cn.br.ReadByte()
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

	switch e.GetField('C') {
	case "23000", "23001", "23502", "23503", "23505", "23514", "23P01":
		return &IntegrityError{pgError: e}, nil
	}
	return e, nil
}

func (cn *conn) Flush() error {
	b := cn.buf.Flush()
	n, err := cn.cn.Write(b)
	if n == len(b) {
		return nil
	}
	return err
}
