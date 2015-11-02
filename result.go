package pg

import (
	"bytes"
	"strconv"
)

var resultSep = []byte{' '}

type Result interface {
	Affected() int
}

// A Result summarizes an executed SQL command.
type result struct {
	affected int
}

func newResult(b []byte) result {
	res := result{
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
func (r result) Affected() int {
	return r.affected
}
