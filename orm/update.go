package orm

import (
	"errors"
	"reflect"

	"github.com/go-pg/pg/internal"
)

func Update(db DB, model ...interface{}) error {
	res, err := NewQuery(db, model...).Update()
	if err != nil {
		return err
	}
	return internal.AssertOneRow(res.RowsAffected())
}

type updateQuery struct {
	q *Query
}

var _ QueryAppender = (*updateQuery)(nil)

func (q updateQuery) Copy() QueryAppender {
	return updateQuery{
		q: q.q.Copy(),
	}
}

func (q updateQuery) Query() *Query {
	return q.q
}

func (q updateQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	var err error

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "UPDATE "...)
	b = q.q.appendFirstTableWithAlias(b)

	b, err = q.mustAppendSet(b)
	if err != nil {
		return nil, err
	}

	if q.q.hasOtherTables() || q.q.modelHasData() {
		b = append(b, " FROM "...)
		b = q.q.appendOtherTables(b)
		b, err = q.q.appendModelData(b)
		if err != nil {
			return nil, err
		}
	}

	b, err = q.q.mustAppendWhere(b)
	if err != nil {
		return nil, err
	}

	if len(q.q.returning) > 0 {
		b = q.q.appendReturning(b)
	}

	return b, nil
}

func (q updateQuery) mustAppendSet(b []byte) ([]byte, error) {
	if len(q.q.set) > 0 {
		b = q.q.appendSet(b)
		return b, nil
	}

	if q.q.model == nil {
		return nil, errors.New("pg: Model is nil")
	}

	b = append(b, " SET "...)

	value := q.q.model.Value()
	var err error
	if value.Kind() == reflect.Struct {
		b, err = q.appendSetStruct(b, value)
	} else {
		b, err = q.appendSetSlice(b, value)
	}
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (q updateQuery) appendSetStruct(b []byte, strct reflect.Value) ([]byte, error) {
	fields, err := q.q.getFields()
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		fields = q.q.model.Table().Columns
	}

	for i, field := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		b = append(b, field.Column...)
		b = append(b, " = "...)
		b = field.AppendValue(b, strct, 1)
	}
	return b, nil
}

func (q updateQuery) appendSetSlice(b []byte, slice reflect.Value) ([]byte, error) {
	fields, err := q.q.getFields()
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		fields = q.q.model.Table().Columns
	}

	for i, field := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		b = append(b, field.Column...)
		b = append(b, " = "...)
		b = append(b, "_data."...)
		b = append(b, field.Column...)
	}
	return b, nil
}
