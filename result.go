package pg

import (
	"bytes"
	"strconv"
)

var resultSep = []byte{' '}

// A Result summarizes an executed SQL command.
type Result struct {
	affected int
}

func newResult(b []byte) *Result {
	res := &Result{
		affected: -1,
	}
	ind := bytes.LastIndex(b, resultSep)
	if ind == -1 {
		return res
	}
	affected, err := strconv.Atoi(string(b[ind+1 : len(b)-1]))
	if err == nil {
		res.affected = affected
	}
	return res
}

// Affected returns the number of rows affected by SELECT, INSERT, UPDATE, or
// DELETE queries. It returns -1 when query can't possibly affect any rows,
// e.g. in case of CREATE or SHOW queries.
func (r *Result) Affected() int {
	return r.affected
}
