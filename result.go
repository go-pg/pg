package pg

import (
	"strconv"
)

type Result struct {
	tags [][]byte
}

func (r *Result) Affected() int64 {
	n, _ := strconv.ParseInt(string(r.tags[len(r.tags)-1]), 10, 64)
	return n
}
