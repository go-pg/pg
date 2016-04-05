package orm

import (
	"strconv"

	"gopkg.in/pg.v4/types"
)

type dber interface {
	Exec(q interface{}, params ...interface{}) (types.Result, error)
	ExecOne(q interface{}, params ...interface{}) (types.Result, error)
	Query(coll, query interface{}, params ...interface{}) (types.Result, error)
	QueryOne(model, query interface{}, params ...interface{}) (types.Result, error)
}

type selectQuery struct {
	*Query
}

var _ QueryAppender = (*selectQuery)(nil)

func (sel selectQuery) AppendQuery(b []byte, params []interface{}) ([]byte, error) {
	b = append(b, "SELECT "...)
	if sel.columns == nil {
		b = types.AppendField(b, sel.model.Table().ModelName, 1)
		b = append(b, ".*"...)
	} else {
		b = append(b, sel.columns...)
	}

	b = append(b, " FROM "...)
	b = append(b, sel.tables...)

	if sel.join != nil {
		b = append(b, ' ')
		b = append(b, sel.join...)
	}

	if sel.where != nil {
		b = append(b, " WHERE "...)
		b = append(b, sel.where...)
	}

	if sel.order != nil {
		b = append(b, " ORDER BY "...)
		b = append(b, sel.order...)
	}

	if sel.limit != 0 {
		b = append(b, " LIMIT "...)
		b = strconv.AppendInt(b, int64(sel.limit), 10)
	}

	if sel.offset != 0 {
		b = append(b, " OFFSET "...)
		b = strconv.AppendInt(b, int64(sel.offset), 10)
	}

	return b, nil
}
