package orm

import (
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
	q           *Query
	omitZero    bool
	placeholder bool
}

var _ QueryAppender = (*updateQuery)(nil)

func (q *updateQuery) Copy() *updateQuery {
	return &updateQuery{
		q:           q.q.Copy(),
		omitZero:    q.omitZero,
		placeholder: q.placeholder,
	}
}

func (q *updateQuery) Query() *Query {
	return q.q
}

func (q *updateQuery) AppendTemplate(b []byte) ([]byte, error) {
	cp := q.Copy()
	cp.q = cp.q.Formatter(dummyFormatter{})
	cp.placeholder = true
	return cp.AppendQuery(b)
}

func (q *updateQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	if len(q.q.with) > 0 {
		b = q.q.appendWith(b)
	}

	b = append(b, "UPDATE "...)
	b = q.q.appendFirstTableWithAlias(b)

	b, err := q.mustAppendSet(b)
	if err != nil {
		return nil, err
	}

	isSliceModelWithData := q.q.isSliceModelWithData()
	if isSliceModelWithData || q.q.hasMultiTables() {
		b = append(b, " FROM "...)
		b = q.q.appendOtherTables(b)

		if isSliceModelWithData {
			b, err = q.appendSliceModelData(b)
			if err != nil {
				return nil, err
			}
		}
	}

	b, err = q.mustAppendWhere(b, isSliceModelWithData)
	if err != nil {
		return nil, err
	}

	if len(q.q.returning) > 0 {
		b = q.q.appendReturning(b)
	}

	return b, q.q.stickyErr
}

func (q *updateQuery) mustAppendWhere(b []byte, isSliceModelWithData bool) ([]byte, error) {
	b = append(b, " WHERE "...)

	if isSliceModelWithData {
		if !q.q.hasModel() {
			return nil, errModelNil
		}

		table := q.q.model.Table()
		if len(table.PKs) > 0 {
			b = appendWhereColumnAndColumn(b, table.Alias, table.PKs)
			if q.q.hasWhere() {
				b = append(b, " AND "...)
				b = q.q.appendWhere(b)
			}
			return b, nil
		}
	}

	return q.q.mustAppendWhere(b)
}

func (q *updateQuery) mustAppendSet(b []byte) ([]byte, error) {
	if len(q.q.set) > 0 {
		b = q.q.appendSet(b)
		return b, nil
	}

	if !q.q.hasModel() {
		return nil, errModelNil
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

func (q *updateQuery) appendSetStruct(b []byte, strct reflect.Value) ([]byte, error) {
	fields, err := q.q.getFields()
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		fields = q.q.model.Table().DataFields
	}

	pos := len(b)
	for _, f := range fields {
		omitZero := f.OmitZero() && f.IsZeroValue(strct)
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

		if q.placeholder {
			b = append(b, '?')
		} else {
			b = f.AppendValue(b, strct, 1)
		}
	}

	return b, nil
}

func (q *updateQuery) appendSetSlice(b []byte, slice reflect.Value) ([]byte, error) {
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

func (q *updateQuery) appendSliceModelData(b []byte) ([]byte, error) {
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

func (q *updateQuery) appendSliceValues(b []byte, fields []*Field, slice reflect.Value) []byte {
	b = append(b, "(VALUES ("...)

	if q.placeholder {
		b = q.appendValues(b, fields, reflect.Value{})
	} else {
		for i := 0; i < slice.Len(); i++ {
			if i > 0 {
				b = append(b, "), ("...)
			}
			b = q.appendValues(b, fields, slice.Index(i))
		}
	}

	b = append(b, ")) AS _data("...)
	b = appendColumns(b, "", fields)
	b = append(b, ")"...)

	return b
}

func (q *updateQuery) appendValues(b []byte, fields []*Field, strct reflect.Value) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		app, ok := q.q.modelValues[f.SQLName]
		if ok {
			b = app.AppendFormat(b, q.q)
			continue
		}

		if q.placeholder {
			b = append(b, '?')
		} else {
			b = f.AppendValue(b, indirect(strct), 1)
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
