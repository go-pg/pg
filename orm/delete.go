package orm

import "github.com/go-pg/pg/internal"

func Delete(db DB, model interface{}) error {
	res, err := NewQuery(db, model).Delete()
	if err != nil {
		return err
	}
	return internal.AssertOneRow(res.RowsAffected())
}

type deleteQuery struct {
	q *Query
}

var _ QueryAppender = (*deleteQuery)(nil)

func (q deleteQuery) Copy() QueryAppender {
	return deleteQuery{
		q: q.q.Copy(),
	}
}

func (q deleteQuery) Query() *Query {
	return q.q
}

func (q deleteQuery) AppendQuery(b []byte) ([]byte, error) {
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

	b = append(b, "DELETE FROM "...)
	b = q.q.appendFirstTable(b)

	if q.q.hasOtherTables() {
		b = append(b, " USING "...)
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
