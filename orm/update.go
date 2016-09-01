package orm

func Update(db DB, v interface{}) error {
	q := NewQuery(db, v)
	if q.err != nil {
		return q.err
	}
	_, err := db.ExecOne(updateQuery{q}, q.model)
	return err
}

type updateQuery struct {
	*Query
}

var _ QueryAppender = (*updateQuery)(nil)

func (upd updateQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	var err error

	b = append(b, "UPDATE "...)
	b = upd.appendTables(b)

	b, err = upd.appendSet(b)
	if err != nil {
		return nil, err
	}

	b, err = upd.appendWhere(b)
	if err != nil {
		return nil, err
	}

	if len(upd.returning) > 0 {
		b = append(b, " RETURNING "...)
		b = append(b, upd.returning...)
	}

	return b, nil
}
