package orm

import "gopkg.in/pg.v5/internal"

func Delete(db DB, model interface{}) error {
	res, err := NewQuery(db, model).Delete()
	if err != nil {
		return err
	}
	return internal.AssertOneRow(res.Affected())
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
