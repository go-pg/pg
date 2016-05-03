package orm

func Delete(db dber, v interface{}) error {
	q := NewQuery(db, v)
	if q.err != nil {
		return q.err
	}
	_, err := db.ExecOne(deleteModel{q}, q.model)
	return err
}

type deleteModel struct {
	*Query
}

var _ QueryAppender = (*deleteModel)(nil)

func (del deleteModel) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	b = append(b, "DELETE FROM "...)
	b = append(b, del.tableName...)
	return del.appendWhere(b)
}
