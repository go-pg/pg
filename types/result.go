package types

import (
	"bytes"
	"strconv"

	"gopkg.in/pg.v4/internal"
)

// A Result summarizes an executed SQL command.
type Result struct {
	affected int
}

func ParseResult(b []byte) *Result {
	res := Result{
		affected: -1,
	}
	ind := bytes.LastIndexByte(b, ' ')
	if ind == -1 {
		return &res
	}
	s := internal.BytesToString(b[ind+1 : len(b)-1])
	affected, err := strconv.Atoi(s)
	if err == nil {
		res.affected = affected
	}
	return &res
}

// Affected returns the number of rows affected by SELECT, INSERT, UPDATE, or
// DELETE queries. It returns -1 when query can't possibly affect any rows,
// e.g. in case of CREATE or SHOW queries.
func (r Result) Affected() int {
	return r.affected
}
