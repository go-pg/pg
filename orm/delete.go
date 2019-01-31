package orm

import (
	"github.com/go-pg/pg/internal"
)

func Delete(db DB, model interface{}) error {
	res, err := NewQuery(db, model).WherePK().Delete()
	if err != nil {
		return err
	}
	return internal.AssertOneRow(res.RowsAffected())
}

func ForceDelete(db DB, model interface{}) error {
	res, err := NewQuery(db, model).WherePK().ForceDelete()
	if err != nil {
		return err
	}
	return internal.AssertOneRow(res.RowsAffected())
}

type deleteQuery struct {
	q           *Query
	placeholder bool
}

var _ QueryAppender = (*deleteQuery)(nil)

func (q *deleteQuery) Copy() *deleteQuery {
	return &deleteQuery{
		q:           q.q.Copy(),
		placeholder: q.placeholder,
	}
}

func (q *deleteQuery) Query() *Query {
	return q.q
}

func (q *deleteQuery) AppendTemplate(b []byte) ([]byte, error) {
	cp := q.Copy()
	cp.q = cp.q.Formatter(dummyFormatter{})
	cp.placeholder = true
	return cp.AppendQuery(b)
}

func (q *deleteQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	if len(q.q.with) > 0 {
		b = q.q.appendWith(b)
	}

	b = append(b, "DELETE FROM "...)
	b = q.q.appendFirstTableWithAlias(b)

	if q.q.hasMultiTables() {
		b = append(b, " USING "...)
		b = q.q.appendOtherTables(b)
	}

	b = append(b, " WHERE "...)
	value := q.q.model.Value()
	if q.q.isSliceModelWithData() {
		table := q.q.model.Table()
		err := table.checkPKs()
		if err != nil {
			return nil, err
		}

		b = appendColumnAndSliceValue(q.q, b, value, table.Alias, table.PKs)

		if q.q.hasWhere() {
			b = append(b, " AND "...)
			b = q.q.appendWhere(b)
		}
	} else {
		var err error
		b, err = q.q.mustAppendWhere(b)
		if err != nil {
			return nil, err
		}
	}

	if len(q.q.returning) > 0 {
		b = q.q.appendReturning(b)
	}

	return b, q.q.stickyErr
}
