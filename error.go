package pg

import (
	"io"
	"net"

	"gopkg.in/pg.v4/internal"
)

var (
	ErrNoRows    = internal.ErrNoRows
	ErrMultiRows = internal.ErrMultiRows

	errSSLNotSupported = internal.Errorf("pg: SSL is not enabled on the server")

	errClosed         = internal.Errorf("pg: database is closed")
	errTxDone         = internal.Errorf("pg: transaction has already been committed or rolled back")
	errStmtClosed     = internal.Errorf("pg: statement is closed")
	errListenerClosed = internal.Errorf("pg: listener is closed")
)

type Error interface {
	Field(byte) string
	IntegrityViolation() bool
}

var _ Error = (*internal.PGError)(nil)

func isBadConn(err error, allowTimeout bool) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(internal.Error); ok {
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
