package orm

import (
	"errors"

	"gopkg.in/pg.v4/types"
)

func Delete(db dber, v interface{}) error {
	model, err := NewTableModel(v)
	if err != nil {
		return err
	}
	_, err = db.Exec(deleteModel{TableModel: model}, model)
	return err
}

type deleteModel struct {
	*TableModel
}

func (del deleteModel) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	strct := del.Value()
	for _, pk := range del.Table.PKs {
		if pk.IsEmpty(strct) {
			return nil, errors.New("pg: primary key is empty")
		}
	}

	b = append(b, "DELETE FROM "...)
	b = types.AppendField(b, del.Table.Name, true)

	b = append(b, " WHERE "...)
	b = appendFieldValue(b, strct, del.Table.PKs)

	return b, nil
}

type deleteQuery struct {
	*Query
}

func (del deleteQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	b = append(b, "DELETE FROM "...)
	b = types.AppendField(b, del.model.Table.Name, true)

	b = append(b, " WHERE "...)
	b = appendString(b, " AND ", del.wheres...)

	return b, nil
}
