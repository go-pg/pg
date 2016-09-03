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

func (del deleteQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	b = append(b, "DELETE FROM "...)
	b = del.appendTables(b)
	return del.appendWhere(b)
}
