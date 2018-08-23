package orm

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/types"
)

func Update(db DB, model interface{}) error {
	res, err := NewQuery(db, model).WherePK().Update()
	if err != nil {
		return err
	}
	return internal.AssertOneRow(res.RowsAffected())
}

type updateQuery struct {
	q        *Query
	omitZero bool
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

	isSliceModel := q.q.isSliceModel()
	if q.q.hasMultiTables() || isSliceModel {
		b = append(b, " FROM "...)
		b = q.q.appendOtherTables(b)

		if isSliceModel {
			b, err = q.appendSliceModelData(b)
			if err != nil {
				return nil, err
			}
		}
	}

	b = append(b, " WHERE "...)
	if isSliceModel {
		table := q.q.model.Table()
		b = appendWhereColumnAndColumn(b, table.Alias, table.PKs)

		if q.q.hasWhere() {
			b = append(b, " AND "...)
			b = q.q.appendWhere(b)
		}
	} else {
		b, err = q.q.mustAppendWhere(b)
		if err != nil {
			return nil, err
		}
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
		return nil, errors.New("pg: Model(nil)")
	}

	b = append(b, " SET "...)

	value := q.q.model.Value()
	var err error
	if value.Kind() == reflect.Struct {
		b, err = q.appendSetStruct(b, value)
	} else {
		if value.Len() > 0 {
			b, err = q.appendSetSlice(b, value)
		} else {
			err = fmt.Errorf("pg: can't bulk-update empty slice %s", value.Type())
		}
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
		fields = q.q.model.Table().DataFields
	}

	pos := len(b)
	for _, f := range fields {
		omitZero := f.OmitZero() && f.IsZero(strct)
		if omitZero && q.omitZero {
			continue
		}

		if len(b) != pos {
			b = append(b, ", "...)
			pos = len(b)
		}

		b = append(b, f.Column...)
		b = append(b, " = "...)

		app, ok := q.q.modelValues[f.SQLName]
		if ok {
			b = app.AppendFormat(b, q.q)
			continue
		}

		if f.OmitZero() && f.IsZero(strct) {
			b = append(b, "NULL"...)
		} else {
			b = f.AppendValue(b, strct, 1)
		}
	}

	return b, nil
}

func (q updateQuery) appendSetSlice(b []byte, slice reflect.Value) ([]byte, error) {
	fields, err := q.q.getFields()
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		fields = q.q.model.Table().DataFields
	}

	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		b = append(b, f.Column...)
		b = append(b, " = "...)
		b = append(b, "_data."...)
		b = append(b, f.Column...)
	}

	return b, nil
}

func (q updateQuery) appendSliceModelData(b []byte) ([]byte, error) {
	columns, err := q.q.getDataFields()
	if err != nil {
		return nil, err
	}

	if len(columns) > 0 {
		columns = append(columns, q.q.model.Table().PKs...)
	} else {
		columns = q.q.model.Table().Fields
	}

	return q.appendSliceValues(b, columns, q.q.model.Value()), nil
}

func (q updateQuery) appendSliceValues(b []byte, fields []*Field, slice reflect.Value) []byte {
	b = append(b, "(VALUES ("...)
	for i := 0; i < slice.Len(); i++ {
		el := indirect(slice.Index(i))
		b = q.appendValues(b, fields, el)
		if i != slice.Len()-1 {
			b = append(b, "), ("...)
		}
	}
	b = append(b, ")) AS _data("...)
	b = appendColumns(b, "", fields)
	b = append(b, ")"...)
	return b
}

func (q updateQuery) appendValues(b []byte, fields []*Field, strct reflect.Value) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		app, ok := q.q.modelValues[f.SQLName]
		if ok {
			b = app.AppendFormat(b, q.q)
			continue
		}

		if f.OmitZero() && f.IsZero(strct) {
			b = append(b, "NULL"...)
		} else {
			b = f.AppendValue(b, strct, 1)
		}
		if f.HasFlag(customTypeFlag) {
			b = append(b, "::"...)
			b = append(b, f.SQLType...)
		}
	}
	return b
}

func appendWhereColumnAndColumn(b []byte, alias types.Q, fields []*Field) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, " AND "...)
		}
		b = append(b, alias...)
		b = append(b, '.')
		b = append(b, f.Column...)
		b = append(b, " = _data."...)
		b = append(b, f.Column...)
	}
	return b
}
