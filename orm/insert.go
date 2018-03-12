package orm

import (
	"errors"
	"reflect"
)

func Insert(db DB, v ...interface{}) error {
	_, err := NewQuery(db, v...).Insert()
	return err
}

type insertQuery struct {
	q               *Query
	returningFields []*Field
}

var _ QueryAppender = (*insertQuery)(nil)

func (q insertQuery) Copy() QueryAppender {
	return insertQuery{
		q: q.q.Copy(),
	}
}

func (q insertQuery) Query() *Query {
	return q.q
}

func (q insertQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}
	if q.q.model == nil {
		return nil, errors.New("pg: Model is nil")
	}

	table := q.q.model.Table()
	value := q.q.model.Value()
	var err error

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "INSERT INTO "...)
	if q.q.onConflict != nil {
		b = q.q.appendFirstTableWithAlias(b)
	} else {
		b = q.q.appendFirstTable(b)
	}

	if q.q.hasOtherTables() {
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
			fields = table.Fields
		}

		b = append(b, " ("...)
		b = appendColumns(b, fields)
		b = append(b, ") VALUES ("...)
		if value.Kind() == reflect.Struct {
			b = q.appendValues(b, fields, value)
		} else {
			if value.Len() == 0 {
				return nil, errors.New("pg: can't bulk-insert empty slice")
			}

			for i := 0; i < value.Len(); i++ {
				el := value.Index(i)
				if el.Kind() == reflect.Interface {
					el = el.Elem()
				}
				b = q.appendValues(b, fields, reflect.Indirect(el))
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
			}

			if len(q.q.updWhere) > 0 {
				b = q.q.appendUpdWhere(b)
			}
		}
	}

	if len(q.q.returning) > 0 {
		b = q.q.appendReturning(b)
	} else if len(q.returningFields) > 0 {
		b = q.appendReturningFields(b, q.returningFields)
	}

	return b, nil
}

func (q *insertQuery) appendValues(b []byte, fields []*Field, v reflect.Value) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		app, ok := q.q.values[f.SQLName]
		if ok {
			b = app.AppendFormat(b, q.q)
			continue
		}

		if f.OmitZero(v) {
			b = append(b, "DEFAULT"...)
			q.addReturningField(f)
		} else {
			b = f.AppendValue(b, v, 1)
		}
	}
	return b
}

func (ins *insertQuery) addReturningField(field *Field) {
	for _, f := range ins.returningFields {
		if f == field {
			return
		}
	}
	ins.returningFields = append(ins.returningFields, field)
}

func (insertQuery) appendReturningFields(b []byte, fields []*Field) []byte {
	b = append(b, " RETURNING "...)
	b = appendColumns(b, fields)
	return b
}
