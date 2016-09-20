package types

import (
	"bytes"
	"strconv"

	"gopkg.in/pg.v5/internal"
)

// A Result summarizes an executed SQL command.
type Result struct {
	affected int
	returned int
}

func NewResult(b []byte, returned int) *Result {
	res := Result{
		affected: -1,
		returned: returned,
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

// RowsAffected returns the number of rows affected by SELECT, INSERT, UPDATE,
// or DELETE queries. It returns -1 when query can't possibly affect any rows,
// e.g. in case of CREATE or SHOW queries.
func (r Result) RowsAffected() int {
	return r.affected
}

// RowsReturned returns the number of rows returned by the query.
func (r Result) RowsReturned() int {
	return r.returned
}
