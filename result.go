package pg

import (
	"bytes"
	"strconv"
)

var resultSep = []byte{' '}

type Result struct {
	b []byte
}

func newResult(b []byte) *Result {
	return &Result{b}
}

func (r *Result) Affected() int {
	ind := bytes.LastIndex(r.b, resultSep)
	if ind == -1 {
		return 0
	}
	n, _ := strconv.Atoi(string(r.b[ind+1 : len(r.b)-1]))
	return n
}
