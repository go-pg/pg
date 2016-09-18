package orm

func Delete(db DB, v interface{}) error {
	q := NewQuery(db, v)
	if q.stickyErr != nil {
		return q.stickyErr
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
	return del.mustAppendWhere(b)
}
