package pg

import (
	"bufio"
	"context"
	"crypto/md5" //nolint
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"mellium.im/sasl"

	"github.com/go-pg/pg/v10/internal"
	"github.com/go-pg/pg/v10/internal/pool"
	"github.com/go-pg/pg/v10/orm"
	"github.com/go-pg/pg/v10/types"
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

	saslInitialResponseMsg        = 'p'
	authenticationSASLContinueMsg = 'R'
	saslResponseMsg               = 'p'
	authenticationSASLFinalMsg    = 'R'

	authenticationOK                = 0
	authenticationCleartextPassword = 3
	authenticationMD5Password       = 5
	authenticationSASL              = 10

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

	syncMsg  = 'S'
	flushMsg = 'H'

	closeMsg         = 'C'
	closeCompleteMsg = '3'

	copyInResponseMsg  = 'G'
	copyOutResponseMsg = 'H'
	copyDataMsg        = 'd'
	copyDoneMsg        = 'c'
)

var errEmptyQuery = internal.Errorf("pg: query is empty")

func (db *baseDB) startup(
	c context.Context, cn *pool.Conn, user, password, database, appName string,
) error {
	err := cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeStartupMsg(wb, user, database, appName)
		return nil
	})
	if err != nil {
		return err
	}

	return cn.WithReader(c, db.opt.ReadTimeout, func(rd *pool.BufReader) error {
		for {
			typ, msgLen, err := readMessageType(rd)
			if err != nil {
				return err
			}

			switch typ {
			case backendKeyDataMsg:
				processID, err := readInt32(rd)
				if err != nil {
					return err
				}
				secretKey, err := readInt32(rd)
				if err != nil {
					return err
				}
				cn.ProcessID = processID
				cn.SecretKey = secretKey
			case parameterStatusMsg:
				if err := logParameterStatus(rd, msgLen); err != nil {
					return err
				}
			case authenticationOKMsg:
				err := db.auth(c, cn, rd, user, password)
				if err != nil {
					return err
				}
			case readyForQueryMsg:
				_, err := rd.ReadN(msgLen)
				return err
			case errorResponseMsg:
				e, err := readError(rd)
				if err != nil {
					return err
				}
				return e
			default:
				return fmt.Errorf("pg: unknown startup message response: %q", typ)
			}
		}
	})
}

func (db *baseDB) enableSSL(c context.Context, cn *pool.Conn, tlsConf *tls.Config) error {
	err := cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writeSSLMsg(wb)
		return nil
	})
	if err != nil {
		return err
	}

	err = cn.WithReader(c, db.opt.ReadTimeout, func(rd *pool.BufReader) error {
		c, err := rd.ReadByte()
		if err != nil {
			return err
		}
		if c != 'S' {
			return errors.New("pg: SSL is not enabled on the server")
		}
		return nil
	})
	if err != nil {
		return err
	}

	cn.SetNetConn(tls.Client(cn.NetConn(), tlsConf))
	return nil
}

func (db *baseDB) auth(
	c context.Context, cn *pool.Conn, rd *pool.BufReader, user, password string,
) error {
	num, err := readInt32(rd)
	if err != nil {
		return err
	}

	switch num {
	case authenticationOK:
		return nil
	case authenticationCleartextPassword:
		return db.authCleartext(c, cn, rd, password)
	case authenticationMD5Password:
		return db.authMD5(c, cn, rd, user, password)
	case authenticationSASL:
		return db.authSASL(c, cn, rd, user, password)
	default:
		return fmt.Errorf("pg: unknown authentication message response: %q", num)
	}
}

func (db *baseDB) authCleartext(
	c context.Context, cn *pool.Conn, rd *pool.BufReader, password string,
) error {
	err := cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writePasswordMsg(wb, password)
		return nil
	})
	if err != nil {
		return err
	}
	return readAuthOK(rd)
}

func (db *baseDB) authMD5(
	c context.Context, cn *pool.Conn, rd *pool.BufReader, user, password string,
) error {
	b, err := rd.ReadN(4)
	if err != nil {
		return err
	}

	secret := "md5" + md5s(md5s(password+user)+string(b))
	err = cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		writePasswordMsg(wb, secret)
		return nil
	})
	if err != nil {
		return err
	}

	return readAuthOK(rd)
}

func readAuthOK(rd *pool.BufReader) error {
	c, _, err := readMessageType(rd)
	if err != nil {
		return err
	}

	switch c {
	case authenticationOKMsg:
		c0, err := readInt32(rd)
		if err != nil {
			return err
		}
		if c0 != 0 {
			return fmt.Errorf("pg: unexpected authentication code: %q", c0)
		}
		return nil
	case errorResponseMsg:
		e, err := readError(rd)
		if err != nil {
			return err
		}
		return e
	default:
		return fmt.Errorf("pg: unknown password message response: %q", c)
	}
}

func (db *baseDB) authSASL(
	c context.Context, cn *pool.Conn, rd *pool.BufReader, user, password string,
) error {
	s, err := readString(rd)
	if err != nil {
		return err
	}
	if s != "SCRAM-SHA-256" {
		return fmt.Errorf("pg: SASL: got %q, wanted %q", s, "SCRAM-SHA-256")
	}

	c0, err := rd.ReadByte()
	if err != nil {
		return err
	}
	if c0 != 0 {
		return fmt.Errorf("pg: SASL: got %q, wanted %q", c0, 0)
	}

	creds := sasl.Credentials(func() (Username, Password, Identity []byte) {
		return []byte(user), []byte(password), nil
	})
	client := sasl.NewClient(sasl.ScramSha256, creds)

	_, resp, err := client.Step(nil)
	if err != nil {
		return err
	}

	err = cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
		wb.StartMessage(saslInitialResponseMsg)
		wb.WriteString("SCRAM-SHA-256")
		wb.WriteInt32(int32(len(resp)))
		_, err := wb.Write(resp)
		if err != nil {
			return err
		}
		wb.FinishMessage()
		return nil
	})
	if err != nil {
		return err
	}

	typ, n, err := readMessageType(rd)
	if err != nil {
		return err
	}

	switch typ {
	case authenticationSASLContinueMsg:
		c11, err := readInt32(rd)
		if err != nil {
			return err
		}
		if c11 != 11 {
			return fmt.Errorf("pg: SASL: got %q, wanted %q", typ, 11)
		}

		b, err := rd.ReadN(n - 4)
		if err != nil {
			return err
		}

		_, resp, err = client.Step(b)
		if err != nil {
			return err
		}

		err = cn.WithWriter(c, db.opt.WriteTimeout, func(wb *pool.WriteBuffer) error {
			wb.StartMessage(saslResponseMsg)
			_, err := wb.Write(resp)
			if err != nil {
				return err
			}
			wb.FinishMessage()
			return nil
		})
		if err != nil {
			return err
		}

		return readAuthSASLFinal(rd, client)
	case errorResponseMsg:
		e, err := readError(rd)
		if err != nil {
			return err
		}
		return e
	default:
		return fmt.Errorf(
			"pg: SASL: got %q, wanted %q", typ, authenticationSASLContinueMsg)
	}
}

func readAuthSASLFinal(rd *pool.BufReader, client *sasl.Negotiator) error {
	c, n, err := readMessageType(rd)
	if err != nil {
		return err
	}

	switch c {
	case authenticationSASLFinalMsg:
		c12, err := readInt32(rd)
		if err != nil {
			return err
		}
		if c12 != 12 {
			return fmt.Errorf("pg: SASL: got %q, wanted %q", c, 12)
		}

		b, err := rd.ReadN(n - 4)
		if err != nil {
			return err
		}

		_, _, err = client.Step(b)
		if err != nil {
			return err
		}

		if client.State() != sasl.ValidServerResponse {
			return fmt.Errorf("pg: SASL: state=%q, wanted %q",
				client.State(), sasl.ValidServerResponse)
		}
	case errorResponseMsg:
		e, err := readError(rd)
		if err != nil {
			return err
		}
		return e
	default:
		return fmt.Errorf(
			"pg: SASL: got %q, wanted %q", c, authenticationSASLFinalMsg)
	}

	return readAuthOK(rd)
}

func md5s(s string) string {
	//nolint
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

func writeStartupMsg(buf *pool.WriteBuffer, user, database, appName string) {
	buf.StartMessage(0)
	buf.WriteInt32(196608)
	buf.WriteString("user")
	buf.WriteString(user)
	buf.WriteString("database")
	buf.WriteString(database)
	if appName != "" {
		buf.WriteString("application_name")
		buf.WriteString(appName)
	}
	buf.WriteString("")
	buf.FinishMessage()
}

func writeSSLMsg(buf *pool.WriteBuffer) {
	buf.StartMessage(0)
	buf.WriteInt32(80877103)
	buf.FinishMessage()
}

func writePasswordMsg(buf *pool.WriteBuffer, password string) {
	buf.StartMessage(passwordMessageMsg)
	buf.WriteString(password)
	buf.FinishMessage()
}

func writeFlushMsg(buf *pool.WriteBuffer) {
	buf.StartMessage(flushMsg)
	buf.FinishMessage()
}

func writeCancelRequestMsg(buf *pool.WriteBuffer, processID, secretKey int32) {
	buf.StartMessage(0)
	buf.WriteInt32(80877102)
	buf.WriteInt32(processID)
	buf.WriteInt32(secretKey)
	buf.FinishMessage()
}

func writeQueryMsg(
	buf *pool.WriteBuffer,
	fmter orm.QueryFormatter,
	query interface{},
	params ...interface{},
) error {
	buf.StartMessage(queryMsg)
	bytes, err := appendQuery(fmter, buf.Bytes, query, params...)
	if err != nil {
		return err
	}
	buf.Bytes = bytes
	err = buf.WriteByte(0x0)
	if err != nil {
		return err
	}
	buf.FinishMessage()
	return nil
}

func appendQuery(fmter orm.QueryFormatter, dst []byte, query interface{}, params ...interface{}) ([]byte, error) {
	switch query := query.(type) {
	case orm.QueryAppender:
		if v, ok := fmter.(*orm.Formatter); ok {
			fmter = v.WithModel(query)
		}
		return query.AppendQuery(fmter, dst)
	case string:
		if len(params) > 0 {
			model, ok := params[len(params)-1].(orm.TableModel)
			if ok {
				if v, ok := fmter.(*orm.Formatter); ok {
					fmter = v.WithTableModel(model)
					params = params[:len(params)-1]
				}
			}
		}
		return fmter.FormatQuery(dst, query, params...), nil
	default:
		return nil, fmt.Errorf("pg: can't append %T", query)
	}
}

func writeSyncMsg(buf *pool.WriteBuffer) {
	buf.StartMessage(syncMsg)
	buf.FinishMessage()
}

func writeParseDescribeSyncMsg(buf *pool.WriteBuffer, name, q string) {
	buf.StartMessage(parseMsg)
	buf.WriteString(name)
	buf.WriteString(q)
	buf.WriteInt16(0)
	buf.FinishMessage()

	buf.StartMessage(describeMsg)
	buf.WriteByte('S') //nolint
	buf.WriteString(name)
	buf.FinishMessage()

	writeSyncMsg(buf)
}

func readParseDescribeSync(rd *pool.BufReader) ([][]byte, error) {
	var columns [][]byte
	var firstErr error
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return nil, err
		}
		switch c {
		case parseCompleteMsg:
			_, err = rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case rowDescriptionMsg: // Response to the DESCRIBE message.
			columns, err = readRowDescription(rd, nil)
			if err != nil {
				return nil, err
			}
		case parameterDescriptionMsg: // Response to the DESCRIBE message.
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case noDataMsg: // Response to the DESCRIBE message.
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case readyForQueryMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if firstErr != nil {
				return nil, firstErr
			}
			return columns, err
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return nil, err
			}
			if firstErr == nil {
				firstErr = e
			}
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readParseDescribeSync: unexpected message %q", c)
		}
	}
}

// Writes BIND, EXECUTE and SYNC messages.
func writeBindExecuteMsg(buf *pool.WriteBuffer, name string, params ...interface{}) error {
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

	writeSyncMsg(buf)

	return nil
}

func writeCloseMsg(buf *pool.WriteBuffer, name string) {
	buf.StartMessage(closeMsg)
	buf.WriteByte('S') //nolint
	buf.WriteString(name)
	buf.FinishMessage()
}

func readCloseCompleteMsg(rd *pool.BufReader) error {
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return err
		}
		switch c {
		case closeCompleteMsg:
			_, err := rd.ReadN(msgLen)
			return err
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return err
			}
			return e
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return err
			}
		default:
			return fmt.Errorf("pg: readCloseCompleteMsg: unexpected message %q", c)
		}
	}
}

func readSimpleQuery(rd *pool.BufReader) (*result, error) {
	var res result
	var firstErr error
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return nil, err
		}

		switch c {
		case commandCompleteMsg:
			b, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if err := res.parse(b); err != nil && firstErr == nil {
				firstErr = err
			}
		case readyForQueryMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if firstErr != nil {
				return nil, firstErr
			}
			return &res, nil
		case rowDescriptionMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case dataRowMsg:
			if _, err := rd.Discard(msgLen); err != nil {
				return nil, err
			}
			res.returned++
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return nil, err
			}
			if firstErr == nil {
				firstErr = e
			}
		case emptyQueryResponseMsg:
			if firstErr == nil {
				firstErr = errEmptyQuery
			}
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readSimpleQuery: unexpected message %q", c)
		}
	}
}

func readExtQuery(rd *pool.BufReader) (*result, error) {
	var res result
	var firstErr error
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return nil, err
		}

		switch c {
		case bindCompleteMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case dataRowMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			res.returned++
		case commandCompleteMsg: // Response to the EXECUTE message.
			b, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if err := res.parse(b); err != nil && firstErr == nil {
				firstErr = err
			}
		case readyForQueryMsg: // Response to the SYNC message.
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if firstErr != nil {
				return nil, firstErr
			}
			return &res, nil
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return nil, err
			}
			if firstErr == nil {
				firstErr = e
			}
		case emptyQueryResponseMsg:
			if firstErr == nil {
				firstErr = errEmptyQuery
			}
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readExtQuery: unexpected message %q", c)
		}
	}
}

func readRowDescription(rd *pool.BufReader, columns [][]byte) ([][]byte, error) {
	colNum, err := readInt16(rd)
	if err != nil {
		return nil, err
	}

	columns = setByteSliceLen(columns, int(colNum))
	for i := 0; i < int(colNum); i++ {
		b, err := rd.ReadSlice(0)
		if err != nil {
			return nil, err
		}
		columns[i] = append(columns[i][:0], b[:len(b)-1]...)

		_, err = rd.ReadN(18)
		if err != nil {
			return nil, err
		}
	}

	return columns, nil
}

func setByteSliceLen(b [][]byte, n int) [][]byte {
	if n <= cap(b) {
		return b[:n]
	}
	b = b[:cap(b)]
	b = append(b, make([][]byte, n-cap(b))...)
	return b
}

func readDataRow(
	ctx context.Context, rd *pool.BufReader, scanner orm.ColumnScanner, columns [][]byte,
) error {
	colNum, err := readInt16(rd)
	if err != nil {
		return err
	}

	if h, ok := scanner.(orm.BeforeScanHook); ok {
		if err := h.BeforeScan(ctx); err != nil {
			return err
		}
	}

	var firstErr error

	for colIdx := int16(0); colIdx < colNum; colIdx++ {
		n, err := readInt32(rd)
		if err != nil {
			return err
		}

		column := internal.BytesToString(columns[colIdx])
		var colRd types.Reader
		if n >= 0 {
			bytesRd := rd.BytesReader(int(n))
			if bytesRd != nil {
				colRd = bytesRd
			} else {
				rd.SetAvailable(int(n))
				colRd = rd
			}
		} else {
			colRd = rd.BytesReader(0)
		}

		err = scanner.ScanColumn(int(colIdx), column, colRd, int(n))
		if err != nil && firstErr == nil {
			firstErr = internal.Errorf(err.Error())
		}

		if rd == colRd {
			if rd.Available() > 0 {
				_, err = rd.Discard(rd.Available())
				if err != nil && firstErr == nil {
					firstErr = err
				}
			}
			rd.SetAvailable(-1)
		}
	}

	if h, ok := scanner.(orm.AfterScanHook); ok {
		if err := h.AfterScan(ctx); err != nil {
			return err
		}
	}

	return firstErr
}

func newModel(mod interface{}) (orm.Model, error) {
	m, err := orm.NewModel(mod)
	if err != nil {
		return nil, err
	}
	return m, m.Init()
}

func readSimpleQueryData(
	ctx context.Context, rd *pool.BufReader, mod interface{},
) (*result, error) {
	var res result
	var firstErr error
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return nil, err
		}

		switch c {
		case rowDescriptionMsg:
			rd.Columns, err = readRowDescription(rd, rd.Columns[:0])
			if err != nil {
				return nil, err
			}

			if res.model == nil {
				var err error
				res.model, err = newModel(mod)
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					res.model = Discard
				}
			}
		case dataRowMsg:
			scanner := res.model.NextColumnScanner()
			if err := readDataRow(ctx, rd, scanner, rd.Columns); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else if err := res.model.AddColumnScanner(scanner); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}

			res.returned++
		case commandCompleteMsg:
			b, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if err := res.parse(b); err != nil && firstErr == nil {
				firstErr = err
			}
		case readyForQueryMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if firstErr != nil {
				return nil, firstErr
			}
			return &res, nil
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return nil, err
			}
			if firstErr == nil {
				firstErr = e
			}
		case emptyQueryResponseMsg:
			if firstErr == nil {
				firstErr = errEmptyQuery
			}
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readSimpleQueryData: unexpected message %q", c)
		}
	}
}

func readExtQueryData(
	ctx context.Context, rd *pool.BufReader, mod interface{}, columns [][]byte,
) (*result, error) {
	var res result
	var firstErr error
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return nil, err
		}

		switch c {
		case bindCompleteMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case dataRowMsg:
			if res.model == nil {
				var err error
				res.model, err = newModel(mod)
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					res.model = Discard
				}
			}

			scanner := res.model.NextColumnScanner()
			if err := readDataRow(ctx, rd, scanner, columns); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else if err := res.model.AddColumnScanner(scanner); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}

			res.returned++
		case commandCompleteMsg: // Response to the EXECUTE message.
			b, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if err := res.parse(b); err != nil && firstErr == nil {
				firstErr = err
			}
		case readyForQueryMsg: // Response to the SYNC message.
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if firstErr != nil {
				return nil, firstErr
			}
			return &res, nil
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return nil, err
			}
			if firstErr == nil {
				firstErr = e
			}
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readExtQueryData: unexpected message %q", c)
		}
	}
}

func readCopyInResponse(rd *pool.BufReader) error {
	var firstErr error
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return err
		}

		switch c {
		case copyInResponseMsg:
			_, err := rd.ReadN(msgLen)
			return err
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return err
			}
			if firstErr == nil {
				firstErr = e
			}
		case readyForQueryMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return err
			}
			return firstErr
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return err
			}
		default:
			return fmt.Errorf("pg: readCopyInResponse: unexpected message %q", c)
		}
	}
}

func readCopyOutResponse(rd *pool.BufReader) error {
	var firstErr error
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return err
		}

		switch c {
		case copyOutResponseMsg:
			_, err := rd.ReadN(msgLen)
			return err
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return err
			}
			if firstErr == nil {
				firstErr = e
			}
		case readyForQueryMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return err
			}
			return firstErr
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return err
			}
		default:
			return fmt.Errorf("pg: readCopyOutResponse: unexpected message %q", c)
		}
	}
}

func readCopyData(rd *pool.BufReader, w io.Writer) (*result, error) {
	var res result
	var firstErr error
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return nil, err
		}

		switch c {
		case copyDataMsg:
			for msgLen > 0 {
				b, err := rd.ReadN(msgLen)
				if err != nil && err != bufio.ErrBufferFull {
					return nil, err
				}

				_, err = w.Write(b)
				if err != nil {
					return nil, err
				}

				msgLen -= len(b)
			}
		case copyDoneMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
		case commandCompleteMsg:
			b, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if err := res.parse(b); err != nil && firstErr == nil {
				firstErr = err
			}
		case readyForQueryMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if firstErr != nil {
				return nil, firstErr
			}
			return &res, nil
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return nil, err
			}
			return nil, e
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readCopyData: unexpected message %q", c)
		}
	}
}

func writeCopyData(buf *pool.WriteBuffer, r io.Reader) error {
	buf.StartMessage(copyDataMsg)
	_, err := buf.ReadFrom(r)
	buf.FinishMessage()
	return err
}

func writeCopyDone(buf *pool.WriteBuffer) {
	buf.StartMessage(copyDoneMsg)
	buf.FinishMessage()
}

func readReadyForQuery(rd *pool.BufReader) (*result, error) {
	var res result
	var firstErr error
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return nil, err
		}

		switch c {
		case commandCompleteMsg:
			b, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if err := res.parse(b); err != nil && firstErr == nil {
				firstErr = err
			}
		case readyForQueryMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return nil, err
			}
			if firstErr != nil {
				return nil, firstErr
			}
			return &res, nil
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return nil, err
			}
			if firstErr == nil {
				firstErr = e
			}
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return nil, err
			}
		case parameterStatusMsg:
			if err := logParameterStatus(rd, msgLen); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("pg: readReadyForQueryOrError: unexpected message %q", c)
		}
	}
}

func readNotification(rd *pool.BufReader) (channel, payload string, err error) {
	for {
		c, msgLen, err := readMessageType(rd)
		if err != nil {
			return "", "", err
		}

		switch c {
		case commandCompleteMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return "", "", err
			}
		case readyForQueryMsg:
			_, err := rd.ReadN(msgLen)
			if err != nil {
				return "", "", err
			}
		case errorResponseMsg:
			e, err := readError(rd)
			if err != nil {
				return "", "", err
			}
			return "", "", e
		case noticeResponseMsg:
			if err := logNotice(rd, msgLen); err != nil {
				return "", "", err
			}
		case notificationResponseMsg:
			_, err := readInt32(rd)
			if err != nil {
				return "", "", err
			}
			channel, err = readString(rd)
			if err != nil {
				return "", "", err
			}
			payload, err = readString(rd)
			if err != nil {
				return "", "", err
			}
			return channel, payload, nil
		default:
			return "", "", fmt.Errorf("pg: readNotification: unexpected message %q", c)
		}
	}
}

var terminateMessage = []byte{terminateMsg, 0, 0, 0, 4}

func terminateConn(cn *pool.Conn) error {
	// Don't use cn.Buf because it is racy with user code.
	_, err := cn.NetConn().Write(terminateMessage)
	return err
}

//------------------------------------------------------------------------------

func logNotice(rd *pool.BufReader, msgLen int) error {
	_, err := rd.ReadN(msgLen)
	return err
}

func logParameterStatus(rd *pool.BufReader, msgLen int) error {
	_, err := rd.ReadN(msgLen)
	return err
}

func readInt16(rd *pool.BufReader) (int16, error) {
	b, err := rd.ReadN(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func readInt32(rd *pool.BufReader) (int32, error) {
	b, err := rd.ReadN(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func readString(rd *pool.BufReader) (string, error) {
	b, err := rd.ReadSlice(0)
	if err != nil {
		return "", err
	}
	return string(b[:len(b)-1]), nil
}

func readError(rd *pool.BufReader) (error, error) {
	m := make(map[byte]string)
	for {
		c, err := rd.ReadByte()
		if err != nil {
			return nil, err
		}
		if c == 0 {
			break
		}
		s, err := readString(rd)
		if err != nil {
			return nil, err
		}
		m[c] = s
	}
	return internal.NewPGError(m), nil
}

func readMessageType(rd *pool.BufReader) (byte, int, error) {
	c, err := rd.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	l, err := readInt32(rd)
	if err != nil {
		return 0, 0, err
	}
	return c, int(l) - 4, nil
}
