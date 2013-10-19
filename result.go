package pg

import (
	"bytes"
	"strconv"
)

var resultSep = []byte{' '}

type Result struct {
	affected int
}

func newResult(b []byte) *Result {
	res := &Result{}

	ind := bytes.LastIndex(b, resultSep)
	if ind != -1 {
		res.affected, _ = strconv.Atoi(string(b[ind+1 : len(b)-1]))
	}

	return res
}

func (r *Result) Affected() int {
	return r.affected
}
