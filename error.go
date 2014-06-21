package pg

import (
	"fmt"
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

func canRetry(e error) bool {
	if e == nil {
		return false
	}
	// 40001 - serialization_failure
	if pgerr, ok := e.(Error); ok && pgerr.Field('C') != "40001" {
		return false
	}
	if _, ok := e.(dbError); ok {
		return false
	}
	if neterr, ok := e.(net.Error); ok && neterr.Timeout() {
		return false
	}
	return true
}
