package orm

import (
	"bytes"
	"reflect"
)

func Create(db dber, v interface{}) error {
	q := NewQuery(db, v)
	if q.err != nil {
		return q.err
	}
	_, err := db.QueryOne(q.model, insertQuery{q}, q.model)
	return err
}

type insertQuery struct {
	*Query
}

var _ QueryAppender = (*insertQuery)(nil)

func (ins insertQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	table := ins.model.Table()
	strct := ins.model.Value()

	b = append(b, "INSERT INTO "...)
	b = append(b, ins.tableName...)
	b = append(b, " ("...)

	var returning []*Field
	var fields []*Field

	start := len(b)
	for _, f := range table.Fields {
		if (f.Has(PrimaryKeyFlag) || f.OmitEmpty(strct)) && f.IsEmpty(strct) {
			returning = append(returning, f)
			continue
		}
		fields = append(fields, f)
		b = append(b, f.ColName...)
		b = append(b, ", "...)
	}
	if len(b) > start {
		b = b[:len(b)-2]
	}

	b = append(b, ") VALUES ("...)

	for i, f := range fields {
		b = f.AppendValue(b, strct, 1)
		if i != len(fields)-1 {
			b = append(b, ", "...)
		}
	}

	b = append(b, ')')

	if len(ins.onConflict) > 0 {
		b = append(b, " ON CONFLICT "...)
		b = append(b, ins.onConflict...)
		if bytes.HasSuffix(ins.onConflict, []byte("DO UPDATE")) {
			var err error
			b, err = ins.appendSet(b)
			if err != nil {
				return nil, err
			}
		}
	}

	if len(ins.returning) > 0 {
		b = append(b, " RETURNING "...)
		b = append(b, ins.returning...)
	} else if len(returning) > 0 {
		b = appendReturning(b, strct, returning)
	}

	return b, nil
}

func appendReturning(b []byte, v reflect.Value, fields []*Field) []byte {
	b = append(b, " RETURNING "...)
	for i, f := range fields {
		b = append(b, f.ColName...)
		if i != len(fields)-1 {
			b = append(b, ", "...)
		}
	}
	return b
}
