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
	*Query
}

var _ QueryAppender = (*deleteQuery)(nil)

func (q deleteQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	var err error

	if len(q.with) > 0 {
		b, err = q.appendWith(b, "")
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "DELETE FROM "...)
	b = q.appendFirstTable(b)

	if q.hasOtherTables() {
		b = append(b, " USING "...)
		b = q.appendOtherTables(b)
	}

	b, err = q.mustAppendWhere(b)
	if err != nil {
		return nil, err
	}

	if len(q.returning) > 0 {
		b = q.appendReturning(b)
	}

	return b, nil
}
