package orm

import (
	"fmt"
	"reflect"

	"github.com/go-pg/pg/v9/types"
)

func Insert(db DB, model ...interface{}) error {
	_, err := NewQuery(db, model...).Insert()
	return err
}

type insertQuery struct {
	q               *Query
	returningFields []*Field
	placeholder     bool
}

var _ QueryAppender = (*insertQuery)(nil)
var _ queryCommand = (*insertQuery)(nil)

func newInsertQuery(q *Query) *insertQuery {
	return &insertQuery{
		q: q,
	}
}

func (q *insertQuery) Clone() queryCommand {
	return &insertQuery{
		q:           q.q.Clone(),
		placeholder: q.placeholder,
	}
}

func (q *insertQuery) Query() *Query {
	return q.q
}

func (q *insertQuery) AppendTemplate(b []byte) ([]byte, error) {
	cp := q.Clone().(*insertQuery)
	cp.placeholder = true
	return cp.AppendQuery(dummyFormatter{}, b)
}

func (q *insertQuery) AppendQuery(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "INSERT INTO "...)
	if q.q.onConflict != nil {
		b, err = q.q.appendFirstTableWithAlias(fmter, b)
	} else {
		b, err = q.q.appendFirstTable(fmter, b)
	}
	if err != nil {
		return nil, err
	}

	if q.q.hasMultiTables() {
		if q.q.columns != nil {
			b = append(b, " ("...)
			b, err = q.q.appendColumns(fmter, b)
			if err != nil {
				return nil, err
			}
			b = append(b, ")"...)
		}
		b = append(b, " SELECT * FROM "...)
		b, err = q.q.appendOtherTables(fmter, b)
		if err != nil {
			return nil, err
		}
	} else {
		if !q.q.hasModel() {
			return nil, errModelNil
		}

		fields, err := q.q.getFields()
		if err != nil {
			return nil, err
		}

		if len(fields) == 0 {
			fields = q.q.model.Table().Fields
		}
		value := q.q.model.Value()

		b = append(b, " ("...)
		b = q.appendColumns(b, fields)
		b = append(b, ") VALUES ("...)
		if m, ok := q.q.model.(*sliceTableModel); ok {
			if m.sliceLen == 0 {
				err = fmt.Errorf("pg: can't bulk-insert empty slice %s", value.Type())
				return nil, err
			}
			b, err = q.appendSliceValues(fmter, b, fields, value)
			if err != nil {
				return nil, err
			}
		} else {
			b, err = q.appendValues(fmter, b, fields, value)
			if err != nil {
				return nil, err
			}
		}
		b = append(b, ")"...)
	}

	if q.q.onConflict != nil {
		b = append(b, " ON CONFLICT "...)
		b, err = q.q.onConflict.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}

		if q.q.onConflictDoUpdate() {
			if len(q.q.set) > 0 {
				b, err = q.q.appendSet(fmter, b)
				if err != nil {
					return nil, err
				}
			} else {
				fields, err := q.q.getDataFields()
				if err != nil {
					return nil, err
				}

				if len(fields) == 0 {
					fields = q.q.model.Table().DataFields
				}

				b = q.appendSetExcluded(b, fields)
			}

			if len(q.q.updWhere) > 0 {
				b = append(b, " WHERE "...)
				b, err = q.q.appendUpdWhere(fmter, b)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if len(q.q.returning) > 0 {
		b, err = q.q.appendReturning(fmter, b)
		if err != nil {
			return nil, err
		}
	} else if len(q.returningFields) > 0 {
		b = appendReturningFields(b, q.returningFields)
	}

	return b, q.q.stickyErr
}

func (q *insertQuery) appendValues(
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
			q.addReturningField(f)
			continue
		}

		switch {
		case q.placeholder:
			b = append(b, '?')
		case (f.Default != "" || f.NullZero()) && f.HasZeroValue(strct):
			b = append(b, "DEFAULT"...)
			q.addReturningField(f)
		default:
			b = f.AppendValue(b, strct, 1)
		}
	}

	for i, v := range q.q.extraValues {
		if i > 0 || len(fields) > 0 {
			b = append(b, ", "...)
		}

		b, err = v.value.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (q *insertQuery) appendSliceValues(
	fmter QueryFormatter, b []byte, fields []*Field, slice reflect.Value,
) (_ []byte, err error) {
	if q.placeholder {
		return q.appendValues(fmter, b, fields, reflect.Value{})
	}

	for i := 0; i < slice.Len(); i++ {
		if i > 0 {
			b = append(b, "), ("...)
		}
		el := indirect(slice.Index(i))
		b, err = q.appendValues(fmter, b, fields, el)
		if err != nil {
			return nil, err
		}
	}

	for i, v := range q.q.extraValues {
		if i > 0 || len(fields) > 0 {
			b = append(b, ", "...)
		}

		b, err = v.value.AppendQuery(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (q *insertQuery) addReturningField(field *Field) {
	if len(q.q.returning) > 0 {
		return
	}
	for _, f := range q.returningFields {
		if f == field {
			return
		}
	}
	q.returningFields = append(q.returningFields, field)
}

func (q *insertQuery) appendSetExcluded(b []byte, fields []*Field) []byte {
	b = append(b, " SET "...)
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, f.Column...)
		b = append(b, " = EXCLUDED."...)
		b = append(b, f.Column...)
	}
	return b
}

func (q *insertQuery) appendColumns(b []byte, fields []*Field) []byte {
	b = appendColumns(b, "", fields)
	for i, v := range q.q.extraValues {
		if i > 0 || len(fields) > 0 {
			b = append(b, ", "...)
		}
		b = types.AppendIdent(b, v.column, 1)
	}
	return b
}

func appendReturningFields(b []byte, fields []*Field) []byte {
	b = append(b, " RETURNING "...)
	b = appendColumns(b, "", fields)
	return b
}
