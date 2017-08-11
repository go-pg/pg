package orm

import (
	"strconv"
	"strings"
)

func Select(db DB, model interface{}) error {
	q := NewQuery(db, model)
	if err := q.model.Table().checkPKs(); err != nil {
		return err
	}
	q.where = append(q.where, wherePKQuery{q})
	return q.Select()
}

type selectQuery struct {
	q *Query

	count string
}

var _ QueryAppender = (*selectQuery)(nil)

func (q selectQuery) Copy() QueryAppender {
	return selectQuery{
		q:     q.q.Copy(),
		count: q.count,
	}
}

func (q selectQuery) Query() *Query {
	return q.q
}

func (q selectQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	var err error

	cteCount := q.count != "" && (len(q.q.group) > 0 || q.isDistinct())
	if cteCount {
		b = append(b, `WITH "_count_wrapper" AS (`...)
	}

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "SELECT "...)
	if q.count != "" && !cteCount {
		b = append(b, q.count...)
	} else {
		b = q.appendColumns(b)
	}

	if q.q.hasTables() {
		b = append(b, " FROM "...)
		b = q.q.appendTables(b)
	}

	q.q.forEachHasOneJoin(func(j *join) {
		b = append(b, ' ')
		b = j.appendHasOneJoin(q.q.db, b)
	})
	if len(q.q.joins) > 0 {
		for _, f := range q.q.joins {
			b = append(b, ' ')
			b = f.AppendFormat(b, q.q)
		}
	}

	if len(q.q.where) > 0 {
		b = q.q.appendWhere(b)
	}

	if len(q.q.group) > 0 {
		b = append(b, " GROUP BY "...)
		for i, f := range q.q.group {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = f.AppendFormat(b, q.q)
		}
	}

	if len(q.q.having) > 0 {
		b = append(b, " HAVING "...)
		for i, f := range q.q.having {
			if i > 0 {
				b = append(b, " AND "...)
			}
			b = append(b, '(')
			b = f.AppendFormat(b, q.q)
			b = append(b, ')')
		}
	}

	if q.count == "" {
		if len(q.q.order) > 0 {
			b = append(b, " ORDER BY "...)
			for i, f := range q.q.order {
				if i > 0 {
					b = append(b, ", "...)
				}
				b = f.AppendFormat(b, q.q)
			}
		}

		if q.q.limit != 0 {
			b = append(b, " LIMIT "...)
			b = strconv.AppendInt(b, int64(q.q.limit), 10)
		}

		if q.q.offset != 0 {
			b = append(b, " OFFSET "...)
			b = strconv.AppendInt(b, int64(q.q.offset), 10)
		}
	} else if cteCount {
		b = append(b, `) SELECT `...)
		b = append(b, q.count...)
		b = append(b, ` FROM "_count_wrapper"`...)
	}

	return b, nil
}

func (q selectQuery) appendColumns(b []byte) []byte {
	start := len(b)

	if q.q.columns != nil {
		b = q.q.appendColumns(b)
	} else if q.q.hasModel() {
		b = q.appendTableColumns(b, q.q.model.Table())
	} else {
		b = append(b, '*')
	}

	q.q.forEachHasOneJoin(func(j *join) {
		if len(b) != start {
			b = append(b, ", "...)
			start = len(b)
		}

		b = j.appendHasOneColumns(b)

		if len(b) == start {
			b = b[:len(b)-2]
		}
	})

	return b
}

func (q selectQuery) appendTableColumns(b []byte, table *Table) []byte {
	for i, f := range table.Fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, table.Alias...)
		b = append(b, '.')
		b = append(b, f.Column...)
	}
	return b
}

func (q selectQuery) isDistinct() bool {
	for _, column := range q.q.columns {
		column, ok := column.(queryParamsAppender)
		if ok {
			if strings.Contains(column.query, "DISTINCT") ||
				strings.Contains(column.query, "distinct") {
				return true
			}
		}
	}
	return false
}
