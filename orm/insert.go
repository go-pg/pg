package orm

import (
	"fmt"
	"reflect"
)

func Insert(db DB, model ...interface{}) error {
	_, err := NewQuery(db, model...).Insert()
	return err
}

type insertQuery struct {
	q               *Query
	returningFields []*Field
}

var _ QueryAppender = (*insertQuery)(nil)

func (q *insertQuery) Copy() QueryAppender {
	return &insertQuery{
		q: q.q.Copy(),
	}
}

func (q *insertQuery) Query() *Query {
	return q.q
}

func (q *insertQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	if len(q.q.with) > 0 {
		b = q.q.appendWith(b)
	}

	b = append(b, "INSERT INTO "...)
	if q.q.onConflict != nil {
		b = q.q.appendFirstTableWithAlias(b)
	} else {
		b = q.q.appendFirstTable(b)
	}

	if q.q.hasMultiTables() {
		if q.q.columns != nil {
			b = append(b, " ("...)
			b = q.q.appendColumns(b)
			b = append(b, ")"...)
		}
		b = append(b, " SELECT * FROM "...)
		b = q.q.appendOtherTables(b)
	} else {
		fields, err := q.q.getFields()
		if err != nil {
			return nil, err
		}

		if len(fields) == 0 {
			fields = q.q.model.Table().Fields
		}
		value := q.q.model.Value()

		b = append(b, " ("...)
		b = appendColumns(b, "", fields)
		b = append(b, ") VALUES ("...)
		if value.Kind() == reflect.Struct {
			b = q.appendValues(b, fields, value)
		} else {
			if value.Len() == 0 {
				err = fmt.Errorf("pg: can't bulk-insert empty slice %s", value.Type())
				return nil, err
			}

			for i := 0; i < value.Len(); i++ {
				el := indirect(value.Index(i))
				b = q.appendValues(b, fields, el)
				if i != value.Len()-1 {
					b = append(b, "), ("...)
				}
			}
		}
		b = append(b, ")"...)
	}

	if q.q.onConflict != nil {
		b = append(b, " ON CONFLICT "...)
		b = q.q.onConflict.AppendFormat(b, q.q)

		if q.q.onConflictDoUpdate() {
			if len(q.q.set) > 0 {
				b = q.q.appendSet(b)
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
				b = q.q.appendUpdWhere(b)
			}
		}
	}

	if len(q.q.returning) > 0 {
		b = q.q.appendReturning(b)
	} else if len(q.returningFields) > 0 {
		b = appendReturningFields(b, q.returningFields)
	}

	return b, q.q.stickyErr
}

func (q *insertQuery) appendValues(b []byte, fields []*Field, v reflect.Value) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		app, ok := q.q.modelValues[f.SQLName]
		if ok {
			b = app.AppendFormat(b, q.q)
			continue
		}

		if (f.Default != "" || f.OmitZero()) && f.IsZeroValue(v) {
			b = append(b, "DEFAULT"...)
			q.addReturningField(f)
		} else {
			b = f.AppendValue(b, v, 1)
		}
	}
	return b
}

func (q *insertQuery) addReturningField(field *Field) {
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

func appendReturningFields(b []byte, fields []*Field) []byte {
	b = append(b, " RETURNING "...)
	b = appendColumns(b, "", fields)
	return b
}
