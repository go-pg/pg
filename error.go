package pg

import (
	"fmt"
	"io"
	"net"
)

var (
	ErrSSLNotSupported = errorf("pg: SSL is not enabled on the server")

	ErrNoRows    = errorf("pg: no rows in result set")
	ErrMultiRows = errorf("pg: multiple rows in result set")

	errClosed         = errorf("pg: database is closed")
	errTxDone         = errorf("pg: transaction has already been committed or rolled back")
	errStmtClosed     = errorf("pg: statement is closed")
	errListenerClosed = errorf("pg: listener is closed")
)

var (
	_ Error = &pgError{}
	_ Error = &pgError{}
)

type Error interface {
	Field(byte) string
}

type dbError struct {
	s string
}

func errorf(s string, args ...interface{}) dbError {
	return dbError{s: fmt.Sprintf(s, args...)}
}

func (err dbError) Error() string {
	return err.s
}

type pgError struct {
	c map[byte]string
}

func (err *pgError) Field(k byte) string {
	return err.c[k]
}

func (err *pgError) Error() string {
	return fmt.Sprintf(
		"%s #%s %s: %s",
		err.Field('S'), err.Field('C'), err.Field('M'), err.Field('D'),
	)
}

type IntegrityError struct {
	*pgError
}

func isBadConn(err error, allowTimeout bool) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(dbError); ok {
		return false
	}
	if pgErr, ok := err.(Error); ok && pgErr.Field('S') != "FATAL" {
		return false
	}
	if allowTimeout {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		}
	}
	return true
}

func isNetworkError(err error) bool {
	if err == io.EOF {
		return true
	}
	_, ok := err.(net.Error)
	return ok
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	// 40001 - serialization_failure
	if pgerr, ok := err.(Error); ok && pgerr.Field('C') == "40001" {
		return true
	}
	return isNetworkError(err)
}
