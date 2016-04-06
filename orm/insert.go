package orm

import "gopkg.in/pg.v4/types"

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

func (ins insertQuery) AppendQuery(b []byte, params []interface{}) ([]byte, error) {
	table := ins.model.Table()
	strct := ins.model.Value()

	b = append(b, "INSERT INTO "...)
	b = types.AppendField(b, table.Name, 1)
	b = append(b, " ("...)

	start := len(b)
	for _, field := range table.Fields {
		if field.Has(PrimaryKeyFlag) && field.IsEmpty(strct) {
			continue
		}
		b = types.AppendField(b, field.SQLName, 1)
		b = append(b, ", "...)
	}
	if len(b) > start {
		b = b[:len(b)-2]
	}

	b = append(b, ") VALUES ("...)

	start = len(b)
	for _, field := range table.Fields {
		if field.Has(PrimaryKeyFlag) && field.IsEmpty(strct) {
			continue
		}
		b = field.AppendValue(b, strct, 1)
		b = append(b, ", "...)
	}
	if len(b) > start {
		b = b[:len(b)-2]
	}

	b = append(b, ')')

	if len(ins.onConflict) > 0 {
		b = append(b, " ON CONFLICT "...)
		b = append(b, ins.onConflict...)
	}

	if len(ins.returning) > 0 {
		b = append(b, " RETURNING "...)
		b = append(b, ins.returning...)
	} else {
		b = appendReturning(b, strct, table.PKs)
	}

	return b, nil
}
