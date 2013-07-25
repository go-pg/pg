package pg

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type msgType byte

const (
	msgCommandComplete  = msgType('C')
	msgErrorResponse    = msgType('E')
	msgNoticeResponse   = msgType('N')
	msgParameterStatus  = msgType('S')
	msgAuthenticationOK = msgType('R')
	msgBackendKeyData   = msgType('K')
	msgNoData           = msgType('n')
	msgPasswordMessage  = msgType('p')

	msgNotificationResponse = msgType('A')

	msgDescribe             = msgType('D')
	msgParameterDescription = msgType('t')

	msgQuery              = msgType('Q')
	msgReadyForQuery      = msgType('Z')
	msgEmptyQueryResponse = msgType('I')
	msgRowDescription     = msgType('T')
	msgDataRow            = msgType('D')

	msgParse         = msgType('P')
	msgParseComplete = msgType('1')

	msgBind         = msgType('B')
	msgBindComplete = msgType('2')

	msgExecute = msgType('E')

	msgSync = msgType('S')
)

func writeQueryMsg(cn *conn, q string, args ...interface{}) error {
	var err error

	cn.buf.StartMsg(msgQuery)
	cn.buf.B, err = AppendQ(cn.buf.B, q, args...)
	if err != nil {
		return err
	}
	cn.buf.WriteByte(0x0)
	cn.buf.EndMsg()
	return cn.Flush()
}

func writeParseDescribeSyncMsg(cn *conn, q string) error {
	cn.buf.StartMsg(msgParse)
	cn.buf.WriteString("")
	cn.buf.WriteString(q)
	cn.buf.WriteInt16(0)
	cn.buf.EndMsg()

	cn.buf.StartMsg(msgDescribe)
	cn.buf.WriteByte('S')
	cn.buf.WriteString("")
	cn.buf.EndMsg()

	cn.buf.StartMsg(msgSync)
	cn.buf.EndMsg()

	return cn.Flush()
}

func readParseDescribeSync(cn *conn) (columns []string, e error) {
	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return nil, err
		}
		switch c {
		case msgParseComplete:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case msgRowDescription: // Response to the DESCRIBE message.
			columns, err = readRowDescription(cn)
			if err != nil {
				return nil, err
			}
		case msgParameterDescription: // Response to the DESCRIBE message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case msgNoData: // Response to the DESCRIBE message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case msgReadyForQuery:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case msgErrorResponse:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case msgNoticeResponse:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}

// Writes BIND, EXECUTE and SYNC messages.
func writeBindExecuteMsg(cn *conn, args ...interface{}) error {
	cn.buf.StartMsg(msgBind)
	cn.buf.WriteString("")
	cn.buf.WriteString("")
	cn.buf.WriteInt16(0)
	cn.buf.WriteInt16(int16(len(args)))
	for i := 0; i < len(args); i++ {
		pos := len(cn.buf.B)
		cn.buf.Grow(4)
		cn.buf.B = appendValue(cn.buf.B, args[i])
		binary.BigEndian.PutUint32(cn.buf.B[pos:], uint32(len(cn.buf.B)-pos-4))
	}
	cn.buf.WriteInt16(0)
	cn.buf.EndMsg()

	cn.buf.StartMsg(msgExecute)
	cn.buf.WriteString("")
	cn.buf.WriteInt32(0)
	cn.buf.EndMsg()

	cn.buf.StartMsg(msgSync)
	cn.buf.EndMsg()

	return cn.Flush()
}

func readBindMsg(cn *conn) (e error) {
	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return err
		}
		switch c {
		case msgBindComplete:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return err
			}
		case msgReadyForQuery: // This is response to the SYNC message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return err
			}
			return
		case msgErrorResponse:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return err
			}
		case msgNoticeResponse:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}

func readSimpleQueryResult(cn *conn) (res *Result, e error) {
	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return nil, err
		}
		switch c {
		case msgCommandComplete:
			b, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = &Result{
				tags: bytes.Split(b[:len(b)-1], []byte{' '}),
			}
		case msgReadyForQuery:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case msgRowDescription, msgDataRow:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case msgErrorResponse:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case msgNoticeResponse:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}

func readExtQueryResult(cn *conn) (res *Result, e error) {
	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return nil, err
		}
		switch c {
		case msgBindComplete:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case msgCommandComplete: // Response to the EXECUTE message.
			b, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = &Result{
				tags: bytes.Split(b[:len(b)-1], []byte{' '}),
			}
		case msgReadyForQuery: // Response to the SYNC message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case msgErrorResponse:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case msgNoticeResponse:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}

func readRowDescription(cn *conn) ([]string, error) {
	colNum, err := cn.ReadInt16()
	if err != nil {
		return nil, err
	}
	columns := make([]string, colNum)
	for i := int16(0); i < colNum; i++ {
		col, err := cn.ReadString()
		if err != nil {
			return nil, err
		}
		columns[i] = col
		if _, err := cn.br.ReadN(18); err != nil {
			return nil, err
		}
	}
	return columns, nil
}

func readDataRow(cn *conn, f Fabric, columns []string) (interface{}, error) {
	dst := f.New()
	loader, ok := dst.(Loader)
	if !ok {
		var err error
		loader, err = newStructLoader(dst, columns)
		if err != nil {
			return nil, err
		}
	}

	colNum, err := cn.ReadInt16()
	if err != nil {
		return nil, err
	}
	for i := int16(0); i < colNum; i++ {
		colLen, err := cn.ReadInt32()
		if err != nil {
			return nil, err
		}
		var b []byte
		if colLen != -1 {
			b, err = cn.br.ReadN(int(colLen))
			if err != nil {
				return nil, err
			}
		}
		if err := loader.Load(int(i), b); err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func readSimpleQueryData(cn *conn, f Fabric) (res []interface{}, e error) {
	var columns []string
	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return nil, err
		}
		switch c {
		case msgRowDescription:
			columns, err = readRowDescription(cn)
			if err != nil {
				return nil, err
			}
		case msgDataRow:
			dst, err := readDataRow(cn, f, columns)
			if err != nil {
				return nil, err
			}
			res = append(res, dst)
		case msgCommandComplete:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case msgReadyForQuery:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case msgErrorResponse:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case msgNoticeResponse:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}

func readExtQueryData(cn *conn, f Fabric, columns []string) (res []interface{}, e error) {
	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return nil, err
		}
		switch c {
		case msgBindComplete:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case msgDataRow:
			dst, err := readDataRow(cn, f, columns)
			if err != nil {
				return nil, err
			}
			res = append(res, dst)
		case msgCommandComplete: // Response to the EXECUTE message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case msgReadyForQuery: // Response to the SYNC message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case msgErrorResponse:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case msgNoticeResponse:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}
