package pg

import (
	"errors"
	"fmt"
)

var (
	ErrSSLNotSupported = errors.New("pg: SSL is not enabled on the server")
	ErrBadConn         = errors.New("pg: bad connection")
	ErrNoRows          = errors.New("pg: no rows in result set")
	ErrMultiRows       = errors.New("pg: multiple rows in result set")
)

type DBError struct {
	c map[byte]string
}

func (err *DBError) Get(k byte) string {
	return err.c[k]
}

func (err *DBError) Error() string {
	return fmt.Sprintf("%s #%s %s: %s", err.Get('S'), err.Get('C'), err.Get('M'), err.Get('D'))
}

type IntegrityError struct {
	*DBError
}
