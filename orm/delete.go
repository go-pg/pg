package orm

import (
	"reflect"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/types"
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
	if q.q.isSliceModel() && value.Len() > 0 {
		table := q.q.model.Table()
		err := table.checkPKs()
		if err != nil {
			return nil, err
		}

		b = q.appendColumnAndSliceValue(b, value, table.Alias, table.PKs)

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

func (q *deleteQuery) appendColumnAndSliceValue(b []byte, slice reflect.Value, alias types.Q, fields []*Field) []byte {
	if len(fields) > 1 {
		b = append(b, '(')
	}
	b = appendColumns(b, alias, fields)
	if len(fields) > 1 {
		b = append(b, ')')
	}

	b = append(b, " IN ("...)

	for i := 0; i < slice.Len(); i++ {
		if i > 0 {
			b = append(b, ", "...)
		}

		el := indirect(slice.Index(i))

		if len(fields) > 1 {
			b = append(b, '(')
		}
		for i, f := range fields {
			if i > 0 {
				b = append(b, ", "...)
			}
			if q.placeholder {
				b = append(b, '?')
			} else {
				b = f.AppendValue(b, el, 1)
			}
		}
		if len(fields) > 1 {
			b = append(b, ')')
		}
	}

	b = append(b, ')')

	return b
}
