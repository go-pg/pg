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
	*Query
	count string
}

var _ QueryAppender = (*selectQuery)(nil)

func (q selectQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	var err error

	if len(q.with) > 0 {
		b, err = q.appendWith(b, q.count)
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

	if q.hasTables() {
		b = append(b, " FROM "...)
		b = q.appendTables(b)
	}

	q.forEachHasOneJoin(func(j *join) {
		b = append(b, ' ')
		b = j.appendHasOneJoin(b)
	})
	if len(q.joins) > 0 {
		for _, f := range q.joins {
			b = append(b, ' ')
			b = f.AppendFormat(b, q)
		}
	}

	if len(q.where) > 0 {
		b = q.appendWhere(b)
	}

	if len(q.group) > 0 {
		b = append(b, " GROUP BY "...)
		for i, f := range q.group {
			if i > 0 {
				b = append(b, ", "...)
			}
			b = f.AppendFormat(b, q)
		}
	}

	if len(q.having) > 0 {
		b = append(b, " HAVING "...)
		for i, f := range q.having {
			if i > 0 {
				b = append(b, " AND "...)
			}
			b = append(b, '(')
			b = f.AppendFormat(b, q)
			b = append(b, ')')
		}
	}

	if q.count == "" {
		if len(q.order) > 0 {
			b = append(b, " ORDER BY "...)
			for i, f := range q.order {
				if i > 0 {
					b = append(b, ", "...)
				}
				b = f.AppendFormat(b, q)
			}
		}

		if q.limit != 0 {
			b = append(b, " LIMIT "...)
			b = strconv.AppendInt(b, int64(q.limit), 10)
		}

		if q.offset != 0 {
			b = append(b, " OFFSET "...)
			b = strconv.AppendInt(b, int64(q.offset), 10)
		}
	}

	return b, nil
}

func (q selectQuery) appendColumns(b []byte) []byte {
	start := len(b)

	if q.columns != nil {
		b = q.appendQueryColumns(b)
	} else if q.hasModel() {
		b = q.appendModelColumns(b)
	} else {
		b = append(b, '*')
	}

	q.forEachHasOneJoin(func(j *join) {
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
	for i, f := range q.columns {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = f.AppendFormat(b, q)
	}
	return b
}

func (q selectQuery) appendModelColumns(b []byte) []byte {
	for i, f := range q.model.Table().Fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, q.model.Table().Alias...)
		b = append(b, '.')
		b = append(b, f.ColName...)
	}
	return b
}
