package orm

func Delete(db DB, v interface{}) error {
	q := NewQuery(db, v)
	if q.err != nil {
		return q.err
	}
	_, err := db.ExecOne(deleteQuery{q}, q.model)
	return err
}

type deleteQuery struct {
	*Query
}

var _ QueryAppender = (*deleteQuery)(nil)

func (q deleteQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	b = append(b, "DELETE FROM "...)
	b = q.appendTables(b)

	b, err := q.appendWhere(b)
	if err != nil {
		return nil, err
	}

	if len(q.returning) > 0 {
		b = append(b, " RETURNING "...)
		b = append(b, q.returning...)
	}

	return b, nil
}
