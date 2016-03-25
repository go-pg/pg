package internal

import "fmt"

type Error struct {
	s string
}

func Errorf(s string, args ...interface{}) Error {
	return Error{s: fmt.Sprintf(s, args...)}
}

func (err Error) Error() string {
	return err.s
}
