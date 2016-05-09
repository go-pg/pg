package pg

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"gopkg.in/pg.v4/internal"
	"gopkg.in/pg.v4/internal/pool"
	"gopkg.in/pg.v4/orm"
	"gopkg.in/pg.v4/types"
)

const (
	commandCompleteMsg  = 'C'
	errorResponseMsg    = 'E'
	noticeResponseMsg   = 'N'
	parameterStatusMsg  = 'S'
	authenticationOKMsg = 'R'
	backendKeyDataMsg   = 'K'
	noDataMsg           = 'n'
	passwordMessageMsg  = 'p'
	terminateMsg        = 'X'

	notificationResponseMsg = 'A'

	describeMsg             = 'D'
	parameterDescriptionMsg = 't'

	queryMsg              = 'Q'
	readyForQueryMsg      = 'Z'
	emptyQueryResponseMsg = 'I'
	rowDescriptionMsg     = 'T'
	dataRowMsg            = 'D'

	parseMsg         = 'P'
	parseCompleteMsg = '1'

	bindMsg         = 'B'
	bindCompleteMsg = '2'

	executeMsg = 'E'

	syncMsg = 'S'

	copyInResponseMsg  = 'G'
	copyOutResponseMsg = 'H'
	copyDataMsg        = 'd'
	copyDoneMsg        = 'c'
)

func startup(cn *pool.Conn, user, password, database string) error {
	writeStartupMsg(cn.Wr, user, database)
	if err := cn.Wr.Flush(); err != nil {
		return err
	}

	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return err
		}
		switch c {
		case backendKeyDataMsg:
			processId, err := readInt32(cn)
			if err != nil {
				return err
			}
			secretKey, err := readInt32(cn)
			if err != nil {
				return err
			}
			cn.ProcessId = processId
			cn.SecretKey = secretKey
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return err
			}
		case authenticationOKMsg:
			if err := authenticate(cn, user, password); err != nil {
				return err
			}
		case readyForQueryMsg:
			_, err := cn.ReadN(msgLen)
			return err
		case errorResponseMsg:
			e, err := readError(cn)
			if err != nil {
				return err
			}
			return e
		default:
			return fmt.Errorf("pg: unknown startup message response: %q", c)
		}
	}
}

func enableSSL(cn *pool.Conn) error {
	writeSSLMsg(cn.Wr)
	if err := cn.Wr.Flush(); err != nil {
		return err
	}

	b := make([]byte, 1)
	_, err := io.ReadFull(cn.NetConn, b)
	if err != nil {
		return err
	}
	if b[0] != 'S' {
		return errSSLNotSupported
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
	}
	cn.NetConn = tls.Client(cn.NetConn, tlsConf)

	return nil
}

func authenticate(cn *pool.Conn, user, password string) error {
	num, err := readInt32(cn)
	if err != nil {
		return err
	}
	switch num {
	case 0:
		return nil
	case 3:
		writePasswordMsg(cn.Wr, password)
		if err := cn.Wr.Flush(); err != nil {
			return err
		}

		c, _, err := readMessageType(cn)
		if err != nil {
			return err
		}
		switch c {
		case authenticationOKMsg:
			num, err := readInt32(cn)
			if err != nil {
				return err
			}
			if num != 0 {
				return fmt.Errorf("pg: unexpected authentication code: %d", num)
			}
			return nil
		case errorResponseMsg:
			e, err := readError(cn)
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

		secret := "md5" + md5s(md5s(password+user)+string(b))
		writePasswordMsg(cn.Wr, secret)
		if err := cn.Wr.Flush(); err != nil {
			return err
		}

		c, _, err := readMessageType(cn)
		if err != nil {
			return err
		}
		switch c {
		case authenticationOKMsg:
			num, err := readInt32(cn)
			if err != nil {
				return err
			}
			if num != 0 {
				return fmt.Errorf("pg: unexpected authentication code: %d", num)
			}
			return nil
		case errorResponseMsg:
			e, err := readError(cn)
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

func md5s(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func writeStartupMsg(buf *pool.Buffer, user, database string) {
	buf.StartMessage(0)
	buf.WriteInt32(196608)
	buf.WriteString("user")
	buf.WriteString(user)
	buf.WriteString("database")
	buf.WriteString(database)
	buf.WriteString("")
	buf.FinishMessage()
}

func writeSSLMsg(buf *pool.Buffer) {
	buf.StartMessage(0)
	buf.WriteInt32(80877103)
	buf.FinishMessage()
}

func writePasswordMsg(buf *pool.Buffer, password string) {
	buf.StartMessage(passwordMessageMsg)
	buf.WriteString(password)
	buf.FinishMessage()
}

func writeCancelRequestMsg(buf *pool.Buffer, processId, secretKey int32) {
	buf.StartMessage(0)
	buf.WriteInt32(80877102)
	buf.WriteInt32(processId)
	buf.WriteInt32(secretKey)
	buf.FinishMessage()
}

func writeQueryMsg(buf *pool.Buffer, query interface{}, params ...interface{}) error {
	buf.StartMessage(queryMsg)
	bytes, err := appendQuery(buf.Bytes, query, params...)
	if err != nil {
		buf.Reset()
		return err
	}
	if internal.QueryLogger != nil {
		internal.LogQuery(string(bytes[5:]))
	}
	buf.Bytes = bytes
	buf.WriteByte(0x0)
	buf.FinishMessage()
	return nil
}

func appendQuery(dst []byte, query interface{}, params ...interface{}) ([]byte, error) {
	switch query := query.(type) {
	case orm.QueryAppender:
		return query.AppendQuery(dst, params...)
	case string:
		return orm.Formatter{}.Append(dst, query, params...), nil
	default:
		return nil, fmt.Errorf("pg: can't append %T", query)
	}
}

func writeParseDescribeSyncMsg(buf *pool.Buffer, name, q string) {
	buf.StartMessage(parseMsg)
	buf.WriteString(name)
	buf.WriteString(q)
	buf.WriteInt16(0)
	buf.FinishMessage()

	buf.StartMessage(describeMsg)
	buf.WriteByte('S')
	buf.WriteString(name)
	buf.FinishMessage()

	buf.StartMessage(syncMsg)
	buf.FinishMessage()
}

func readParseDescribeSync(cn *pool.Conn) ([]string, error) {
	var columns []string
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return nil, err
		}
		switch c {
		case parseCompleteMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case rowDescriptionMsg: // Response to the DESCRIBE message.
			columns, err = readRowDescription(cn)
			if err != nil {
				return nil, err
			}
		case parameterDescriptionMsg: // Response to the DESCRIBE message.
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case noDataMsg: // Response to the DESCRIBE message.
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case readyForQueryMsg:
			_, err := cn.ReadN(msgLen)
			return columns, err
		case errorResponseMsg:
			e, err := readError(cn)
			if err != nil {
				return nil, err
			}
			return nil, e
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readParseDescribeSync: unexpected message %#x", c)
		}
	}
}

// Writes BIND, EXECUTE and SYNC messages.
func writeBindExecuteMsg(buf *pool.Buffer, name string, params ...interface{}) error {
	const paramLenWidth = 4

	buf.StartMessage(bindMsg)
	buf.WriteString("")
	buf.WriteString(name)
	buf.WriteInt16(0)
	buf.WriteInt16(int16(len(params)))
	for _, param := range params {
		buf.StartParam()
		bytes := types.Append(buf.Bytes, param, 0)
		if bytes != nil {
			buf.Bytes = bytes
			buf.FinishParam()
		} else {
			buf.FinishNullParam()
		}
	}
	buf.WriteInt16(0)
	buf.FinishMessage()

	buf.StartMessage(executeMsg)
	buf.WriteString("")
	buf.WriteInt32(0)
	buf.FinishMessage()

	buf.StartMessage(syncMsg)
	buf.FinishMessage()

	return nil
}

func readBindMsg(cn *pool.Conn) (e error) {
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return err
		}
		switch c {
		case bindCompleteMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return err
			}
		case readyForQueryMsg: // This is response to the SYNC message.
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return err
			}
			return
		case errorResponseMsg:
			var err error
			e, err = readError(cn)
			if err != nil {
				return err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return err
			}
		default:
			if e != nil {
				return e
			}
			return fmt.Errorf("pg: readBindMsg: unexpected message %#x", c)
		}
	}
}

func readSimpleQuery(cn *pool.Conn) (res *types.Result, e error) {
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return nil, err
		}
		switch c {
		case commandCompleteMsg:
			b, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = types.ParseResult(b)
		case readyForQueryMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case rowDescriptionMsg, dataRowMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case errorResponseMsg:
			var err error
			e, err = readError(cn)
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return nil, err
			}
		default:
			if e != nil {
				return nil, e
			}
			return nil, fmt.Errorf("pg: readSimpleQuery: unexpected message %#x", c)
		}
	}
}

func readExtQuery(cn *pool.Conn) (res *types.Result, e error) {
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return nil, err
		}
		switch c {
		case bindCompleteMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case dataRowMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case commandCompleteMsg: // Response to the EXECUTE message.
			b, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = types.ParseResult(b)
		case readyForQueryMsg: // Response to the SYNC message.
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case errorResponseMsg:
			var err error
			e, err = readError(cn)
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return nil, err
			}
		default:
			if e != nil {
				return nil, e
			}
			return nil, fmt.Errorf("pg: readExtQuery: unexpected message %#x", c)
		}
	}
}

func readRowDescription(cn *pool.Conn) ([]string, error) {
	colNum, err := readInt16(cn)
	if err != nil {
		return nil, err
	}
	cols := make([]string, colNum)
	for i := int16(0); i < colNum; i++ {
		col, err := readString(cn)
		if err != nil {
			return nil, err
		}
		cols[i] = col
		if _, err := cn.ReadN(18); err != nil {
			return nil, err
		}
	}
	return cols, nil
}

func readDataRow(cn *pool.Conn, scanner orm.ColumnScanner, columns []string) (scanErr error) {
	colNum, err := readInt16(cn)
	if err != nil {
		return err
	}
	for colIdx := 0; colIdx < int(colNum); colIdx++ {
		l, err := readInt32(cn)
		if err != nil {
			return err
		}
		var b []byte
		if l != -1 {
			b, err = cn.ReadN(int(l))
			if err != nil {
				return err
			}
		}
		if err := scanner.ScanColumn(colIdx, columns[colIdx], b); err != nil {
			scanErr = err
		}

	}
	return scanErr
}

func readSimpleQueryData(cn *pool.Conn, mod interface{}) (res *types.Result, e error) {
	coll, ok := mod.(orm.Collection)
	if !ok {
		coll, e = orm.NewModel(mod)
		if e != nil {
			coll = Discard
		}
	}

	var columns []string
	var model orm.ColumnScanner
	for {
		c, msgLen, err := readMessageType(cn)
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
			model = coll.NewModel()
			if err := readDataRow(cn, model, columns); err != nil {
				e = err
			}
			if err := coll.AddModel(model); err != nil {
				return nil, err
			}
		case commandCompleteMsg:
			b, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = types.ParseResult(b)
		case readyForQueryMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case errorResponseMsg:
			var err error
			e, err = readError(cn)
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return nil, err
			}
		default:
			if e != nil {
				return nil, e
			}
			return nil, fmt.Errorf("pg: readSimpleQueryData: unexpected message %#x", c)
		}
	}
}

func readExtQueryData(cn *pool.Conn, mod interface{}, columns []string) (res *types.Result, e error) {
	coll, ok := mod.(orm.Collection)
	if !ok {
		coll, e = orm.NewModel(mod)
		if e != nil {
			coll = Discard
		}
	}

	var model orm.ColumnScanner
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return nil, err
		}
		switch c {
		case bindCompleteMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case dataRowMsg:
			model = coll.NewModel()
			if err := readDataRow(cn, model, columns); err != nil {
				e = err
			}
			if err := coll.AddModel(model); err != nil {
				return nil, err
			}
		case commandCompleteMsg: // Response to the EXECUTE message.
			b, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = types.ParseResult(b)
		case readyForQueryMsg: // Response to the SYNC message.
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return
		case errorResponseMsg:
			var err error
			e, err = readError(cn)
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return nil, err
			}
		default:
			if e != nil {
				return nil, e
			}
			return nil, fmt.Errorf("pg: readExtQueryData: unexpected message %#x", c)
		}
	}
}

func readCopyInResponse(cn *pool.Conn) error {
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return err
		}
		switch c {
		case copyInResponseMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return err
			}
			return nil
		case errorResponseMsg:
			e, err := readError(cn)
			if err != nil {
				return err
			}
			return e
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return err
			}
		default:
			return fmt.Errorf("pg: readCopyInResponse: unexpected message %#x", c)
		}
	}
}

func readCopyOutResponse(cn *pool.Conn) error {
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return err
		}
		switch c {
		case copyOutResponseMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return err
			}
			return nil
		case errorResponseMsg:
			e, err := readError(cn)
			if err != nil {
				return err
			}
			return e
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return err
			}
		default:
			return fmt.Errorf("pg: readCopyOutResponse: unexpected message %#x", c)
		}
	}
}

func readCopyData(cn *pool.Conn, w io.Writer) (*types.Result, error) {
	var res *types.Result
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return nil, err
		}
		switch c {
		case copyDataMsg:
			b, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}

			_, err = w.Write(b)
			if err != nil {
				return nil, err
			}
		case copyDoneMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case commandCompleteMsg:
			b, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = types.ParseResult(b)
		case readyForQueryMsg:
			_, err := cn.ReadN(msgLen)
			return res, err
		case errorResponseMsg:
			e, err := readError(cn)
			if err != nil {
				return nil, err
			}
			return nil, e
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readCopyData: unexpected message %#x", c)
		}
	}
}

func writeCopyData(buf *pool.Buffer, r io.Reader) (int64, error) {
	buf.StartMessage(copyDataMsg)
	n, err := buf.ReadFrom(r)
	buf.FinishMessage()
	return n, err
}

func writeCopyDone(buf *pool.Buffer) {
	buf.StartMessage(copyDoneMsg)
	buf.FinishMessage()
}

func readReadyForQueryOrError(cn *pool.Conn) (*types.Result, error) {
	var res *types.Result
	var e error
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return nil, err
		}
		switch c {
		case commandCompleteMsg:
			b, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res = types.ParseResult(b)
		case readyForQueryMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			return res, e
		case errorResponseMsg:
			var err error
			e, err = readError(cn)
			if err != nil {
				return nil, err
			}
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(cn, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readReadyForQueryOrError: unexpected message %#x", c)
		}
	}
}

func readNotification(cn *pool.Conn) (channel, payload string, err error) {
	for {
		c, msgLen, err := readMessageType(cn)
		if err != nil {
			return "", "", err
		}

		switch c {
		case commandCompleteMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return "", "", err
			}
		case readyForQueryMsg:
			_, err := cn.ReadN(msgLen)
			if err != nil {
				return "", "", err
			}
		case errorResponseMsg:
			e, err := readError(cn)
			if err != nil {
				return "", "", err
			}
			return "", "", e
		case noticeResponseMsg:
			if err := logNotice(cn, msgLen); err != nil {
				return "", "", err
			}
		case notificationResponseMsg:
			_, err := readInt32(cn)
			if err != nil {
				return "", "", err
			}
			channel, err = readString(cn)
			if err != nil {
				return "", "", err
			}
			payload, err = readString(cn)
			if err != nil {
				return "", "", err
			}
			return channel, payload, nil
		default:
			return "", "", fmt.Errorf("pg: unexpected message %q", c)
		}
	}
}

var terminateMessage = []byte{terminateMsg, 0, 0, 0, 5}

func terminateConn(cn *pool.Conn) error {
	// Don't use cn.Buf because it is racy.
	_, err := cn.Write(terminateMessage)
	return err
}

//------------------------------------------------------------------------------

func readInt16(cn *pool.Conn) (int16, error) {
	b, err := cn.ReadN(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func readInt32(cn *pool.Conn) (int32, error) {
	b, err := cn.ReadN(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func readString(cn *pool.Conn) (string, error) {
	s, err := cn.Rd.ReadString(0)
	if err != nil {
		return "", err
	}
	return s[:len(s)-1], nil
}

func readError(cn *pool.Conn) (error, error) {
	m := make(map[byte]string)
	for {
		c, err := cn.Rd.ReadByte()
		if err != nil {
			return nil, err
		}
		if c == 0 {
			break
		}
		s, err := readString(cn)
		if err != nil {
			return nil, err
		}
		m[c] = s
	}

	return internal.NewPGError(m), nil
}

func readMessageType(cn *pool.Conn) (byte, int, error) {
	c, err := cn.Rd.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	l, err := readInt32(cn)
	if err != nil {
		return 0, 0, err
	}
	return c, int(l) - 4, nil
}

func logNotice(cn *pool.Conn, msgLen int) error {
	_, err := cn.ReadN(msgLen)
	return err
}

func logParameterStatus(cn *pool.Conn, msgLen int) error {
	_, err := cn.ReadN(msgLen)
	return err
}
