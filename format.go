package pg

import (
	"fmt"

	"gopkg.in/pg.v3/orm"
)

func AppendQuery(dst []byte, srci interface{}, params ...interface{}) ([]byte, error) {
	switch src := srci.(type) {
	case orm.QueryAppender:
		return src.AppendQuery(dst, params...)
	case string:
		f := orm.NewFormatter(params)
		return f.Append(dst, src)
	default:
		return nil, fmt.Errorf("pg: can't append %T", srci)
	}
}

func FormatQuery(query string, params ...interface{}) (Q, error) {
	b, err := AppendQuery(nil, query, params...)
	if err != nil {
		return "", err
	}
	return Q(b), nil
}
