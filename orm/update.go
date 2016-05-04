package orm

func Update(db dber, v interface{}) error {
	q := NewQuery(db, v)
	if q.err != nil {
		return q.err
	}
	_, err := db.ExecOne(updateModel{q}, q.model)
	return err
}

type updateModel struct {
	*Query
}

var _ QueryAppender = (*updateModel)(nil)

func (upd updateModel) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	var err error

	b = append(b, "UPDATE "...)
	b = append(b, upd.tableName...)

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
