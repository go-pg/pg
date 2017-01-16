package orm

import (
	"bytes"
	"errors"
	"reflect"
)

func Insert(db DB, v ...interface{}) error {
	_, err := NewQuery(db, v...).Insert()
	return err
}

type insertQuery struct {
	*Query
	returningFields []*Field
}

var _ QueryAppender = (*insertQuery)(nil)

func (q insertQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	if q.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}

	table := q.model.Table()
	value := q.model.Value()

	b = append(b, "INSERT INTO "...)
	if q.onConflict != nil {
		b = q.appendTableNameWithAlias(b)
	} else {
		b = q.appendTableName(b)
	}
	b = append(b, " ("...)

	start := len(b)
	for _, f := range table.Fields {
		b = append(b, f.ColName...)
		b = append(b, ", "...)
	}
	if len(b) > start {
		b = b[:len(b)-2]
	}

	b = append(b, ") VALUES ("...)
	if value.Kind() == reflect.Struct {
		b = q.appendValues(b, table.Fields, value)
	} else {
		for i := 0; i < value.Len(); i++ {
			el := value.Index(i)
			if el.Kind() == reflect.Interface {
				el = el.Elem()
			}
			b = q.appendValues(b, table.Fields, reflect.Indirect(el))
			if i != value.Len()-1 {
				b = append(b, "), ("...)
			}
		}
	}
	b = append(b, ')')

	if q.onConflict != nil {
		b = append(b, " ON CONFLICT "...)
		b = q.onConflict.AppendFormat(b, q)

		if onConflictDoUpdate(b) {
			if len(q.set) > 0 {
				b = q.appendSet(b)
			}

			if len(q.where) > 0 {
				b = q.appendWhere(b)
			}
		}
	}

	if len(q.returning) > 0 {
		b = q.appendReturning(b)
	} else if len(q.returningFields) > 0 {
		b = q.appendReturningFields(b, q.returningFields)
	}

	return b, nil
}

func onConflictDoUpdate(b []byte) bool {
	return bytes.HasSuffix(b, []byte(" DO UPDATE"))
}

func (q *insertQuery) appendValues(b []byte, fields []*Field, v reflect.Value) []byte {
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		if f.OmitEmpty(v) {
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
	for i, f := range fields {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, f.ColName...)
	}
	return b
}
