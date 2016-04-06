package internal

import "fmt"

var (
	ErrNoRows    = Errorf("pg: no rows in result set")
	ErrMultiRows = Errorf("pg: multiple rows in result set")
)

type Error struct {
	s string
}

func Errorf(s string, args ...interface{}) Error {
	return Error{s: fmt.Sprintf(s, args...)}
}

func (err Error) Error() string {
	return err.s
}
