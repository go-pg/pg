package pg

import (
	"errors"
	"fmt"
)

var (
	ErrSSLNotSupported = errors.New("pg: SSL is not enabled on the server")

	ErrNoRows    = &dbError{"pg: no rows in result set"}
	ErrMultiRows = &dbError{"pg: multiple rows in result set"}

	errExpectedPlaceholder   = &dbError{"pg: expected placeholder"}
	errUnexpectedPlaceholder = &dbError{"pg: unexpected placeholder"}
)

var (
	_ Error = &dbError{}
	_ Error = &pgError{}
	_ Error = &IntegrityError{}
)

type Error interface {
	error

	// Marker.
	DBError()
}

type dbError struct {
	s string
}

func (err *dbError) Error() string {
	return err.s
}

func (err *dbError) DBError() {}

type pgError struct {
	c map[byte]string
}

func (err *pgError) DBError() {}

func (err *pgError) GetField(k byte) string {
	return err.c[k]
}

func (err *pgError) Error() string {
	return fmt.Sprintf(
		"%s #%s %s: %s",
		err.GetField('S'), err.GetField('C'), err.GetField('M'), err.GetField('D'),
	)
}

type IntegrityError struct {
	*pgError
}
