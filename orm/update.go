package orm

import (
	"errors"

	"gopkg.in/pg.v4/types"
)

func Update(db dber, v interface{}) error {
	q := NewQuery(db, v)
	_, err := db.Query(q.model, updateModel{Query: q}, q.model)
	return err
}

type updateModel struct {
	*Query
}

func (upd updateModel) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	table := upd.model.Table()
	strct := upd.model.Value()

	for _, pk := range table.PKs {
		if pk.IsEmpty(strct) {
			return nil, errors.New("pg: primary key is empty")
		}
	}

	b = append(b, "UPDATE "...)
	b = types.AppendField(b, table.Name, 1)
	b = append(b, " SET "...)

	if len(upd.columns) > 0 {
		for i, v := range upd.columns {
			column, err := v.AppendValue(nil, 0)
			if err != nil {
				return nil, err
			}

			field, err := table.GetField(string(column))
			if err != nil {
				return nil, err
			}

			b = types.AppendField(b, field.SQLName, 1)
			b = append(b, " = "...)
			b = field.AppendValue(b, strct, 1)
			if i != len(upd.columns)-1 {
				b = append(b, ", "...)
			}
		}
	} else {
		start := len(b)
		for _, field := range table.Fields {
			if field.Has(PrimaryKeyFlag) {
				continue
			}

			b = types.AppendField(b, field.SQLName, 1)
			b = append(b, " = "...)
			b = field.AppendValue(b, strct, 1)
			b = append(b, ", "...)
		}
		if len(b) > start {
			b = b[:len(b)-2]
		}
	}

	b = append(b, " WHERE "...)
	b = appendFieldValue(b, strct, table.PKs)

	if len(upd.returning) > 0 {
		b = append(b, " RETURNING "...)
		b = appendValue(b, ", ", upd.returning...)
	}

	return b, nil
}

type updateQuery struct {
	*Query
	data map[string]interface{}
}

func (upd updateQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	b = append(b, "UPDATE "...)
	b = types.AppendField(b, upd.model.Table().Name, 1)
	b = append(b, " SET "...)

	for fieldName, value := range upd.data {
		b = types.AppendField(b, fieldName, 1)
		b = append(b, " = "...)
		b = types.Append(b, value, 1)
		b = append(b, ", "...)
	}
	if len(upd.data) > 0 {
		b = b[:len(b)-2]
	}

	b = append(b, " WHERE "...)
	b = appendWheres(b, upd.wheres)

	if len(upd.returning) > 0 {
		b = append(b, " RETURNING "...)
		b = appendValue(b, ", ", upd.returning...)
	}

	return b, nil
}
