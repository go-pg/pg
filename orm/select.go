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
var _ queryCommand = (*selectQuery)(nil)

func newSelectQuery(q *Query) *selectQuery {
	return &selectQuery{
		q: q,
	}
}

func (q *selectQuery) Clone() queryCommand {
	return &selectQuery{
		q:     q.q.Clone(),
		count: q.count,
	}
}

func (q *selectQuery) Query() *Query {
	return q.q
}

func (q *selectQuery) AppendTemplate(b []byte) ([]byte, error) {
	return q.AppendQuery(dummyFormatter{}, b)
}

func (q *selectQuery) AppendQuery(fmter QueryFormatter, b []byte) (_ []byte, err error) { //nolint:gocyclo
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	cteCount := q.count != "" && (len(q.q.group) > 0 || q.isDistinct())
	if cteCount {
		b = append(b, `WITH "_count_wrapper" AS (`...)
	}

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "SELECT "...)

	if len(q.q.distinctOn) > 0 {
		b = append(b, "DISTINCT ON ("...)
		for i, app := range q.q.distinctOn {
			if i > 0 {
				b = append(b, ", "...)
			}
			b, err = app.AppendQuery(fmter, b)
		}
		b = append(b, ") "...)
	} else if q.q.distinctOn != nil {
		b = append(b, "DISTINCT "...)
	}

	if q.count != "" && !cteCount {
		b = append(b, q.count...)
	} else {
		b, err = q.appendColumns(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	if q.q.hasTables() {
		b = append(b, " FROM "...)
		b, err = q.appendTables(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	err = q.q.forEachHasOneJoin(func(j *join) error {
		b = append(b, ' ')
		b, err = j.appendHasOneJoin(fmter, b, q.q)
		return err
	})
	if err != nil {
		return nil, err
	}

	if len(q.q.joins) > 0 {
		for _, j := range q.q.joins {
			b = append(b, ' ')
			b, err = j.join.AppendQuery(fmter, b)
			if err != nil {
				return nil, err
			}
			if len(j.on) > 0 {
				b = append(b, " ON "...)
			}
			for i, on := range j.on {
				if i > 0 {
					b = on.AppendSep(b)
				}
				b, err = on.AppendQuery(fmter, b)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if len(q.q.where) > 0 || q.q.isSoftDelete() {
		b = append(b, " WHERE "...)
		b, err = q.q.appendWhere(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	if len(q.q.group) > 0 {
		b = append(b, " GROUP BY "...)
		for i, f := range q.q.group {
			if i > 0 {
				b = append(b, ", "...)
			}
			b, err = f.AppendQuery(fmter, b)
			if err != nil {
				return nil, err
			}
		}
	}

	if len(q.q.having) > 0 {
		b = append(b, " HAVING "...)
		for i, f := range q.q.having {
			if i > 0 {
				b = append(b, " AND "...)
			}
			b = append(b, '(')
			b, err = f.AppendQuery(fmter, b)
			if err != nil {
				return nil, err
			}
			b = append(b, ')')
		}
	}

	for _, u := range q.q.union {
		b = append(b, u.expr...)
		b, err = u.query.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	if q.count == "" {
		if len(q.q.order) > 0 {
			b = append(b, " ORDER BY "...)
			for i, f := range q.q.order {
				if i > 0 {
					b = append(b, ", "...)
				}
				b, err = f.AppendQuery(fmter, b)
				if err != nil {
					return nil, err
				}
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
			b, err = q.q.selFor.AppendQuery(fmter, b)
			if err != nil {
				return nil, err
			}
		}
	} else if cteCount {
		b = append(b, `) SELECT `...)
		b = append(b, q.count...)
		b = append(b, ` FROM "_count_wrapper"`...)
	}

	return b, q.q.stickyErr
}

func (q selectQuery) appendColumns(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	start := len(b)
	switch {
	case q.q.columns != nil:
		b, err = q.q.appendColumns(fmter, b)
		if err != nil {
			return nil, err
		}
	case q.q.hasExplicitModel():
		table := q.q.model.Table()
		b = appendColumns(b, table.Alias, table.Fields)
	default:
		b = append(b, '*')
	}

	err = q.q.forEachHasOneJoin(func(j *join) error {
		if len(b) != start {
			b = append(b, ", "...)
			start = len(b)
		}

		b = j.appendHasOneColumns(b)

		if len(b) == start {
			b = b[:len(b)-2]
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (q *selectQuery) isDistinct() bool {
	if q.q.distinctOn != nil {
		return true
	}
	for _, column := range q.q.columns {
		column, ok := column.(*SafeQueryAppender)
		if ok {
			if strings.Contains(column.query, "DISTINCT") ||
				strings.Contains(column.query, "distinct") {
				return true
			}
		}
	}
	return false
}

func (q *selectQuery) appendTables(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	tables := q.q.tables

	if q.q.modelHasTableName() {
		table := q.q.model.Table()
		b = fmter.FormatQuery(b, string(table.FullNameForSelects))
		if table.Alias != "" {
			b = append(b, " AS "...)
			b = append(b, table.Alias...)
		}

		if len(tables) > 0 {
			b = append(b, ", "...)
		}
	} else if len(tables) > 0 {
		b, err = tables[0].AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
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
		b, err = f.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}
