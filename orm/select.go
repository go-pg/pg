package orm

import (
	"strconv"
	"strings"
)

func Select(db DB, model interface{}) error {
	return NewQuery(db, model).WherePK().Select()
}

type selectQuery struct {
	q     *Query
	count string
}

var _ QueryAppender = (*selectQuery)(nil)

func (q *selectQuery) Copy() *selectQuery {
	return &selectQuery{
		q:     q.q.Copy(),
		count: q.count,
	}
}

func (q *selectQuery) Query() *Query {
	return q.q
}

func (q *selectQuery) AppendTemplate(b []byte) ([]byte, error) {
	cp := q.Copy()
	cp.q = cp.q.Formatter(dummyFormatter{})
	return cp.AppendQuery(b)
}

func (q *selectQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	cteCount := q.count != "" && (len(q.q.group) > 0 || q.isDistinct())
	if cteCount {
		b = append(b, `WITH "_count_wrapper" AS (`...)
	}

	if len(q.q.with) > 0 {
		b = q.q.appendWith(b)
	}

	b = append(b, "SELECT "...)
	if q.count != "" && !cteCount {
		b = append(b, q.count...)
	} else {
		b = q.appendColumns(b)
	}

	if q.q.hasTables() {
		b = append(b, " FROM "...)
		b = q.appendTables(b)
	}

	q.q.forEachHasOneJoin(func(j *join) {
		b = append(b, ' ')
		b = j.appendHasOneJoin(q.q, b)
	})
	hasHasMany := false
	q.q.forEachHasManyJoin(func(j *join) {
		b = append(b, ' ')
		b = j.appendHasManyJoin(q.q, b)
		hasHasMany = true
	})
	if len(q.q.joins) > 0 {
		for _, j := range q.q.joins {
			b = append(b, ' ')
			b = j.join.AppendFormat(b, q.q)
			if len(j.on) > 0 {
				b = append(b, " ON "...)
			}
			for i, on := range j.on {
				if i > 0 {
					b = on.AppendSep(b)
				}
				b = on.AppendFormat(b, q.q)
			}
		}
	}

	if q.q.hasWhere() {
		b = append(b, " WHERE "...)
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
	} else if hasHasMany {
		b = append(b, " GROUP BY "...)
		b = q.appendBaseColumns(b, true)
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

		if q.q.selFor != nil {
			b = append(b, " FOR "...)
			b = q.q.selFor.AppendFormat(b, q.q)
		}
	} else if cteCount {
		b = append(b, `) SELECT `...)
		b = append(b, q.count...)
		b = append(b, ` FROM "_count_wrapper"`...)
	}

	return b, q.q.stickyErr
}

func (q selectQuery) appendBaseColumns(b []byte, excludeAlias bool) []byte {
	start := len(b)
	if q.q.columns != nil {
		b = q.q.appendColumns(b)
	} else if q.q.hasExplicitModel() {
		table := q.q.model.Table()
		b = appendColumns(b, table.Alias, table.Fields)
	} else {
		b = append(b, '*')
	}

	q.q.forEachHasOneJoin(func(j *join) {
		if len(b) != start {
			b = append(b, ", "...)
			start = len(b)
		}

		b = j.appendHasOneColumns(b, excludeAlias)

		if len(b) == start {
			b = b[:len(b)-2]
		}
	})
	return b
}

func (q selectQuery) appendColumns(b []byte) []byte {
	start := len(b)

	b = q.appendBaseColumns(b, false)

	q.q.forEachHasManyJoin(func(j *join) {
		if len(b) != start {
			b = append(b, ", "...)
			start = len(b)
		}

		b = j.appendHasManyColumns(b)

		if len(b) == start {
			b = b[:len(b)-2]
		}
	})

	return b
}

func (q *selectQuery) isDistinct() bool {
	for _, column := range q.q.columns {
		column, ok := column.(*queryParamsAppender)
		if ok {
			if strings.Contains(column.query, "DISTINCT") ||
				strings.Contains(column.query, "distinct") {
				return true
			}
		}
	}
	return false
}

func (q *selectQuery) appendTables(b []byte) []byte {
	tables := q.q.tables

	if q.q.modelHasTableName() {
		table := q.q.model.Table()
		b = q.q.FormatQuery(b, string(table.FullNameForSelects))
		if table.Alias != "" {
			b = append(b, " AS "...)
			b = append(b, table.Alias...)
		}

		if len(tables) > 0 {
			b = append(b, ", "...)
		}
	} else if len(tables) > 0 {
		b = tables[0].AppendFormat(b, q.q)
		if q.q.modelHasTableAlias() {
			b = append(b, " AS "...)
			b = append(b, q.q.model.Table().Alias...)
		}

		tables = tables[1:]
		if len(tables) > 0 {
			b = append(b, ", "...)
		}
	}

	for i, f := range tables {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = f.AppendFormat(b, q.q)
	}

	return b
}
