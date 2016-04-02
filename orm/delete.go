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
	TableModel
}

func (del deleteModel) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	table := del.Table()
	strct := del.Value()

	for _, pk := range table.PKs {
		if pk.IsEmpty(strct) {
			return nil, errors.New("pg: primary key is empty")
		}
	}

	b = append(b, "DELETE FROM "...)
	b = types.AppendField(b, table.Name, 1)

	b = append(b, " WHERE "...)
	b = appendFieldValue(b, strct, table.PKs)

	return b, nil
}

type deleteQuery struct {
	*Query
}

func (del deleteQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	b = append(b, "DELETE FROM "...)
	b = types.AppendField(b, del.model.Table().Name, 1)

	b = append(b, " WHERE "...)
	b = append(b, del.where...)

	return b, nil
}
