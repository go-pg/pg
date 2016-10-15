package orm

import "gopkg.in/pg.v5/internal"

func Delete(db DB, model interface{}) error {
	res, err := NewQuery(db, model).Delete()
	if err != nil {
		return err
	}
	return internal.AssertOneRow(res.RowsAffected())
}

type deleteQuery struct {
	*Query
}

var _ QueryAppender = (*deleteQuery)(nil)

func (q deleteQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	b = append(b, "DELETE FROM "...)
	b = q.appendTables(b)

	b, err := q.mustAppendWhere(b)
	if err != nil {
		return nil, err
	}

	if len(q.returning) > 0 {
		b = q.appendReturning(b)
	}

	return b, nil
}
