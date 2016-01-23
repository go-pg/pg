package orm

import (
	"errors"

	"gopkg.in/pg.v3/types"
)

func Delete(db dber, v interface{}) error {
	model, err := NewModel(v)
	if err != nil {
		return err
	}
	_, err = db.Exec(deleteQuery{Model: model})
	return err
}

type deleteQuery struct {
	*Model
}

func (del deleteQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	strct := del.Value()
	if del.Table.PK.IsEmpty(strct) {
		return nil, errors.New("primary key is empty")
	}

	b = append(b, "DELETE FROM "...)
	b = types.AppendField(b, del.Table.Name, true)
	b = append(b, " WHERE "...)
	b = types.AppendField(b, del.Table.PK.SQLName, true)
	b = append(b, " = "...)
	b = del.Table.PK.AppendValue(b, strct, true)

	return b, nil
}
