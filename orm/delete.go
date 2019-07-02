package orm

import (
	"github.com/go-pg/pg/v9/internal"
)

// Delete deletes a given model from the db
func Delete(db DB, model interface{}) error {
	res, err := NewQuery(db, model).WherePK().Delete()
	if err != nil {
		return err
	}
	return internal.AssertOneRow(res.RowsAffected())
}

// ForceDelete force deletes a given model from the db
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
var _ queryCommand = (*deleteQuery)(nil)

func newDeleteQuery(q *Query) *deleteQuery {
	return &deleteQuery{
		q: q,
	}
}

func (q *deleteQuery) Clone() queryCommand {
	return &deleteQuery{
		q:           q.q.Clone(),
		placeholder: q.placeholder,
	}
}

func (q *deleteQuery) Query() *Query {
	return q.q
}

func (q *deleteQuery) AppendTemplate(b []byte) ([]byte, error) {
	cp := q.Clone().(*deleteQuery)
	cp.placeholder = true
	return cp.AppendQuery(dummyFormatter{}, b)
}

func (q *deleteQuery) AppendQuery(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "DELETE FROM "...)
	b, err = q.q.appendFirstTableWithAlias(fmter, b)
	if err != nil {
		return nil, err
	}

	if q.q.hasMultiTables() {
		b = append(b, " USING "...)
		b, err = q.q.appendOtherTables(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, " WHERE "...)
	value := q.q.model.Value()
	if q.q.isSliceModelWithData() {
		if len(q.q.where) > 0 {
			b, err = q.q.appendWhere(fmter, b)
			if err != nil {
				return nil, err
			}
		} else {
			table := q.q.model.Table()
			err = table.checkPKs()
			if err != nil {
				return nil, err
			}

			b = appendColumnAndSliceValue(fmter, b, value, table.Alias, table.PKs)
		}
	} else {
		b, err = q.q.mustAppendWhere(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	if len(q.q.returning) > 0 {
		b, err = q.q.appendReturning(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, q.q.stickyErr
}
