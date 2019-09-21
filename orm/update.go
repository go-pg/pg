package orm

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/v9/internal"
	"github.com/go-pg/pg/v9/types"
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
var _ queryCommand = (*updateQuery)(nil)

func newUpdateQuery(q *Query, omitZero bool) *updateQuery {
	return &updateQuery{
		q:        q,
		omitZero: omitZero,
	}
}

func (q *updateQuery) Clone() queryCommand {
	return &updateQuery{
		q:           q.q.Clone(),
		omitZero:    q.omitZero,
		placeholder: q.placeholder,
	}
}

func (q *updateQuery) Query() *Query {
	return q.q
}

func (q *updateQuery) AppendTemplate(b []byte) ([]byte, error) {
	cp := q.Clone().(*updateQuery)
	cp.placeholder = true
	return cp.AppendQuery(dummyFormatter{}, b)
}

func (q *updateQuery) AppendQuery(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "UPDATE "...)

	b, err = q.q.appendFirstTableWithAlias(fmter, b)
	if err != nil {
		return nil, err
	}

	b, err = q.mustAppendSet(fmter, b)
	if err != nil {
		return nil, err
	}

	isSliceModelWithData := q.q.isSliceModelWithData()
	if isSliceModelWithData || q.q.hasMultiTables() {
		b = append(b, " FROM "...)
		b, err = q.q.appendOtherTables(fmter, b)
		if err != nil {
			return nil, err
		}

		if isSliceModelWithData {
			b, err = q.appendSliceModelData(fmter, b)
			if err != nil {
				return nil, err
			}
		}
	}

	b, err = q.mustAppendWhere(fmter, b, isSliceModelWithData)
	if err != nil {
		return nil, err
	}

	if len(q.q.returning) > 0 {
		b, err = q.q.appendReturning(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, q.q.stickyErr
}

func (q *updateQuery) mustAppendWhere(
	fmter QueryFormatter, b []byte, isSliceModelWithData bool,
) (_ []byte, err error) {
	b = append(b, " WHERE "...)

	if !isSliceModelWithData {
		return q.q.mustAppendWhere(fmter, b)
	}

	if len(q.q.where) > 0 {
		return q.q.appendWhere(fmter, b)
	}

	table := q.q.model.Table()
	err = table.checkPKs()
	if err != nil {
		return nil, err
	}

	b = appendWhereColumnAndColumn(b, table.Alias, table.PKs)
	return b, nil
}

func (q *updateQuery) mustAppendSet(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if len(q.q.set) > 0 {
		return q.q.appendSet(fmter, b)
	}
	if !q.q.hasModel() {
		return nil, errModelNil
	}

	b = append(b, " SET "...)

	value := q.q.model.Value()
	if value.Kind() == reflect.Struct {
		b, err = q.appendSetStruct(fmter, b, value)
	} else {
		if value.Len() > 0 {
			b, err = q.appendSetSlice(b)
		} else {
			err = fmt.Errorf("pg: can't bulk-update empty slice %s", value.Type())
		}
	}
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (q *updateQuery) appendSetStruct(fmter QueryFormatter, b []byte, strct reflect.Value) ([]byte, error) {
	fields, err := q.q.getFields()
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		fields = q.q.model.Table().DataFields
	}

	pos := len(b)
	for _, f := range fields {
		if q.omitZero && f.HasZeroValue(strct) {
			continue
		}

		if len(b) != pos {
			b = append(b, ", "...)
			pos = len(b)
		}

		b = append(b, f.Column...)
		b = append(b, " = "...)

		if q.placeholder {
			b = append(b, '?')
			continue
		}

		app, ok := q.q.modelValues[f.SQLName]
		if ok {
			b, err = app.AppendQuery(fmter, b)
			if err != nil {
				return nil, err
			}
		} else {
			b = f.AppendValue(b, strct, 1)
		}
	}

	for i, v := range q.q.extraValues {
		if i > 0 || len(fields) > 0 {
			b = append(b, ", "...)
		}

		b = append(b, v.column...)
		b = append(b, " = "...)

		b, err = v.value.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (q *updateQuery) appendSetSlice(b []byte) ([]byte, error) {
	fields, err := q.q.getFields()
	if err != nil {
		return nil, err
	}

	if len(fields) == 0 {
		fields = q.q.model.Table().DataFields
	}

	var table *Table
	if q.omitZero {
		table = q.q.model.Table()
	}

	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		b = append(b, f.Column...)
		b = append(b, " = "...)
		if q.omitZero && table != nil {
			b = append(b, "COALESCE("...)
		}
		b = append(b, "_data."...)
		b = append(b, f.Column...)
		if q.omitZero && table != nil {
			b = append(b, ", "...)
			if table.Alias != table.FullName {
				b = append(b, table.Alias...)
				b = append(b, '.')
			}
			b = append(b, f.Column...)
			b = append(b, ")"...)
		}
	}

	return b, nil
}

func (q *updateQuery) appendSliceModelData(fmter QueryFormatter, b []byte) ([]byte, error) {
	columns, err := q.q.getDataFields()
	if err != nil {
		return nil, err
	}

	if len(columns) > 0 {
		columns = append(columns, q.q.model.Table().PKs...)
	} else {
		columns = q.q.model.Table().Fields
	}

	return q.appendSliceValues(fmter, b, columns, q.q.model.Value())
}

func (q *updateQuery) appendSliceValues(
	fmter QueryFormatter, b []byte, fields []*Field, slice reflect.Value,
) (_ []byte, err error) {
	b = append(b, "(VALUES ("...)

	if q.placeholder {
		b, err = q.appendValues(fmter, b, fields, reflect.Value{})
		if err != nil {
			return nil, err
		}
	} else {
		for i := 0; i < slice.Len(); i++ {
			if i > 0 {
				b = append(b, "), ("...)
			}
			b, err = q.appendValues(fmter, b, fields, slice.Index(i))
			if err != nil {
				return nil, err
			}
		}
	}

	b = append(b, ")) AS _data("...)
	b = appendColumns(b, "", fields)
	b = append(b, ")"...)

	return b, nil
}

func (q *updateQuery) appendValues(
	fmter QueryFormatter, b []byte, fields []*Field, strct reflect.Value,
) (_ []byte, err error) {
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		app, ok := q.q.modelValues[f.SQLName]
		if ok {
			b, err = app.AppendQuery(fmter, b)
			if err != nil {
				return nil, err
			}
			continue
		}

		if q.placeholder {
			b = append(b, '?')
		} else {
			b = f.AppendValue(b, indirect(strct), 1)
		}
		b = append(b, "::"...)
		b = append(b, f.SQLType...)
	}
	return b, nil
}

func appendWhereColumnAndColumn(b []byte, alias types.Safe, fields []*Field) []byte {
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
