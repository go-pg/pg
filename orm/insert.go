package orm

import (
	"errors"
	"reflect"
)

func Create(db DB, v ...interface{}) error {
	_, err := NewQuery(db, v...).Create()
	return err
}

type insertQuery struct {
	*Query
	returningFields []*Field
}

var _ QueryAppender = (*insertQuery)(nil)

func (ins insertQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	if ins.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}

	table := ins.model.Table()
	value := ins.model.Value()

	b = append(b, "INSERT INTO "...)
	if len(ins.onConflict) > 0 {
		b = ins.appendTableNameWithAlias(b)
	} else {
		b = ins.appendTableName(b)
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
		b = ins.appendValues(b, table.Fields, value)
	} else {
		for i := 0; i < value.Len(); i++ {
			el := value.Index(i)
			if el.Kind() == reflect.Interface {
				el = el.Elem()
			}
			b = ins.appendValues(b, table.Fields, reflect.Indirect(el))
			if i != value.Len()-1 {
				b = append(b, "), ("...)
			}
		}
	}
	b = append(b, ')')

	if len(ins.onConflict) > 0 {
		b = append(b, ins.onConflict...)
	}

	if len(ins.returning) > 0 {
		b = append(b, " RETURNING "...)
		b = append(b, ins.returning...)
	} else if len(ins.returningFields) > 0 {
		b = ins.appendReturning(b, ins.returningFields)
	}

	return b, nil
}

func (ins *insertQuery) appendValues(b []byte, fields []*Field, v reflect.Value) []byte {
	for i, f := range fields {
		if ins.omitEmpty(f, v) {
			b = append(b, "DEFAULT"...)
			ins.addReturningField(f)
		} else {
			b = f.AppendValue(b, v, 1)
		}
		if i != len(fields)-1 {
			b = append(b, ", "...)
		}
	}
	return b
}

func (insertQuery) omitEmpty(f *Field, v reflect.Value) bool {
	omit := f.Has(PrimaryKeyFlag)
	if !omit && v.Kind() == reflect.Struct {
		omit = f.OmitEmpty(v)
	}
	if !omit {
		return false
	}
	return f.IsEmpty(v)
}

func (ins *insertQuery) addReturningField(field *Field) {
	for _, f := range ins.returningFields {
		if f == field {
			return
		}
	}
	ins.returningFields = append(ins.returningFields, field)
}

func (insertQuery) appendReturning(b []byte, fields []*Field) []byte {
	b = append(b, " RETURNING "...)
	for i, f := range fields {
		b = append(b, f.ColName...)
		if i != len(fields)-1 {
			b = append(b, ", "...)
		}
	}
	return b
}
