package orm

import "strconv"

func Select(db DB, model interface{}) error {
	q := NewQuery(db, model)
	if err := q.model.Table().checkPKs(); err != nil {
		return err
	}
	q.where = append(q.where, wherePKQuery{q})
	return q.Select()
}

type selectQuery struct {
	q     *Query
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

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(b, q.count)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "SELECT "...)
	if q.count != "" && q.count != "*" {
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
		b = j.appendHasOneJoin(b)
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
	}

	return b, nil
}

func (q selectQuery) appendColumns(b []byte) []byte {
	start := len(b)

	if q.q.columns != nil {
		b = q.appendQueryColumns(b)
	} else if q.q.hasModel() {
		b = q.appendModelColumns(b)
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

func (q selectQuery) appendQueryColumns(b []byte) []byte {
	for i, f := range q.q.columns {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = f.AppendFormat(b, q.q)
	}
	return b
}

func (q selectQuery) appendModelColumns(b []byte) []byte {
	for i, f := range q.q.model.Table().Fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, q.q.model.Table().Alias...)
		b = append(b, '.')
		b = append(b, f.ColName...)
	}
	return b
}
