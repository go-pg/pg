package pg

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/golang/glog"
)

type msgType byte

const (
	commandCompleteMsg  = msgType('C')
	errorResponseMsg    = msgType('E')
	noticeResponseMsg   = msgType('N')
	parameterStatusMsg  = msgType('S')
	authenticationOKMsg = msgType('R')
	backendKeyDataMsg   = msgType('K')
	noDataMsg           = msgType('n')
	passwordMessageMsg  = msgType('p')

	notificationResponseMsg = msgType('A')

	describeMsg             = msgType('D')
	parameterDescriptionMsg = msgType('t')

	queryMsg              = msgType('Q')
	readyForQueryMsg      = msgType('Z')
	emptyQueryResponseMsg = msgType('I')
	rowDescriptionMsg     = msgType('T')
	dataRowMsg            = msgType('D')

	parseMsg         = msgType('P')
	parseCompleteMsg = msgType('1')

	bindMsg         = msgType('B')
	bindCompleteMsg = msgType('2')

	executeMsg = msgType('E')

	syncMsg = msgType('S')
)

var resultSep = []byte{' '}

func logNotice(cn *conn, msgLen int) error {
	if !glog.V(2) {
		_, err := cn.br.ReadN(msgLen)
		return err
	}

	var level string
	var logger func(string, ...interface{})
	for {
		c, err := cn.br.ReadByte()
		if err != nil {
			return err
		}
		if c == 0 {
			break
		}
		s, err := cn.ReadString()
		if err != nil {
			return err
		}

		switch c {
		case 'S':
			level = s
			switch level {
			case "DEBUG", "LOG", "INFO", "NOTICE":
				logger = glog.Infof
			case "WARNING":
				logger = glog.Warningf
			case "EXCEPTION":
				logger = glog.Errorf
			default:
				logger = glog.Fatalf
			}
		case 'M':
			logger("pg %s message: %s", level, s)
		}
	}

	return nil
}

func writeQueryMsg(buf *buffer, q string, args ...interface{}) (err error) {
	buf.StartMsg(queryMsg)
	buf.B, err = AppendQ(buf.B, q, args...)
	if err != nil {
		return err
	}
	buf.WriteByte(0x0)
	buf.EndMsg()
	return nil
}

func writeParseDescribeSyncMsg(buf *buffer, q string) error {
	buf.StartMsg(parseMsg)
	buf.WriteString("")
	buf.WriteString(q)
	buf.WriteInt16(0)
	buf.EndMsg()

	buf.StartMsg(describeMsg)
	buf.WriteByte('S')
	buf.WriteString("")
	buf.EndMsg()

	buf.StartMsg(syncMsg)
	buf.EndMsg()

	return nil
}

func readParseDescribeSync(cn *conn) (columns []string, e error) {
	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return nil, err
		}
		switch c {
		case parseCompleteMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case rowDescriptionMsg: // Response to the DESCRIBE message.
			columns, err = readRowDescription(cn)
			if err != nil {
				return nil, err
			}
		case parameterDescriptionMsg: // Response to the DESCRIBE message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case noDataMsg: // Response to the DESCRIBE message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case readyForQueryMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case errorResponseMsg:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}

// Writes BIND, EXECUTE and SYNC messages.
func writeBindExecuteMsg(buf *buffer, args ...interface{}) error {
	buf.StartMsg(bindMsg)
	buf.WriteString("")
	buf.WriteString("")
	buf.WriteInt16(0)
	buf.WriteInt16(int16(len(args)))
	for i := 0; i < len(args); i++ {
		pos := len(buf.B)
		buf.Grow(4)
		buf.B = appendValue(buf.B, args[i])
		binary.BigEndian.PutUint32(buf.B[pos:], uint32(len(buf.B)-pos-4))
	}
	buf.WriteInt16(0)
	buf.EndMsg()

	buf.StartMsg(executeMsg)
	buf.WriteString("")
	buf.WriteInt32(0)
	buf.EndMsg()

	buf.StartMsg(syncMsg)
	buf.EndMsg()

	return nil
}

func readBindMsg(cn *conn) (e error) {
	for {
		c, msgLen, err := cn.ReadMsgType()
		if err != nil {
			return err
		}
		switch c {
		case bindCompleteMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return err
			}
		case readyForQueryMsg: // This is response to the SYNC message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return err
			}
			return
		case errorResponseMsg:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
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
		case commandCompleteMsg:
			b, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = &Result{
				tags: bytes.Split(b[:len(b)-1], resultSep),
			}
		case readyForQueryMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case rowDescriptionMsg, dataRowMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case errorResponseMsg:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
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
		case bindCompleteMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case commandCompleteMsg: // Response to the EXECUTE message.
			b, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = &Result{
				tags: bytes.Split(b[:len(b)-1], resultSep),
			}
		case readyForQueryMsg: // Response to the SYNC message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case errorResponseMsg:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
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
	cols := make([]string, colNum)
	for i := int16(0); i < colNum; i++ {
		col, err := cn.ReadString()
		if err != nil {
			return nil, err
		}
		cols[i] = col
		if _, err := cn.br.ReadN(18); err != nil {
			return nil, err
		}
	}
	return cols, nil
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
	for i := 0; i < int(colNum); i++ {
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
		if err := loader.Load(i, b); err != nil {
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
		case rowDescriptionMsg:
			columns, err = readRowDescription(cn)
			if err != nil {
				return nil, err
			}
		case dataRowMsg:
			row, err := readDataRow(cn, f, columns)
			if err != nil {
				return nil, err
			}
			res = append(res, row)
		case commandCompleteMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case readyForQueryMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case errorResponseMsg:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
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
		case bindCompleteMsg:
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case dataRowMsg:
			dst, err := readDataRow(cn, f, columns)
			if err != nil {
				return nil, err
			}
			res = append(res, dst)
		case commandCompleteMsg: // Response to the EXECUTE message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case readyForQueryMsg: // Response to the SYNC message.
			_, err := cn.br.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case errorResponseMsg:
			var err error
			e, err = cn.ReadError()
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}
