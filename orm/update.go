package orm

import (
	"errors"

	"github.com/go-pg/pg/internal"
)

func Update(db DB, model interface{}) error {
	res, err := NewQuery(db, model).Update()
	if err != nil {
		return err
	}
	return internal.AssertOneRow(res.RowsAffected())
}

type updateQuery struct {
	q *Query
}

var _ QueryAppender = (*updateQuery)(nil)

func (q updateQuery) Copy() QueryAppender {
	return updateQuery{
		q: q.q.Copy(),
	}
}

func (q updateQuery) Query() *Query {
	return q.q
}

func (q updateQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	var err error

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(b, "")
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "UPDATE "...)
	b = q.q.appendFirstTable(b)

	b, err = q.mustAppendSet(b)
	if err != nil {
		return nil, err
	}

	if q.q.hasOtherTables() {
		b = append(b, " FROM "...)
		b = q.q.appendOtherTables(b)
	}

	b, err = q.q.mustAppendWhere(b)
	if err != nil {
		return nil, err
	}

	if len(q.q.returning) > 0 {
		b = q.q.appendReturning(b)
	}

	return b, nil
}

func (q updateQuery) mustAppendSet(b []byte) ([]byte, error) {
	if len(q.q.set) > 0 {
		b = q.q.appendSet(b)
		return b, nil
	}

	if q.q.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}

	b = append(b, " SET "...)

	table := q.q.model.Table()
	strct := q.q.model.Value()

	if fields := q.q.getFields(); len(fields) > 0 {
		for i, fieldName := range fields {
			field, err := table.GetField(fieldName)
			if err != nil {
				return nil, err
			}

			if i > 0 {
				b = append(b, ", "...)
			}

			b = append(b, field.ColName...)
			b = append(b, " = "...)
			b = field.AppendValue(b, strct, 1)
		}
		return b, nil
	}

	start := len(b)
	for _, field := range table.Fields {
		if field.Has(PrimaryKeyFlag) {
			continue
		}

		b = append(b, field.ColName...)
		b = append(b, " = "...)
		b = field.AppendValue(b, strct, 1)
		b = append(b, ", "...)
	}
	if len(b) > start {
		b = b[:len(b)-2]
	}
	return b, nil
}
