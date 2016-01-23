package orm

import (
	"errors"

	"gopkg.in/pg.v3/types"
)

func Update(db dber, v interface{}) error {
	model, err := NewModel(v)
	if err != nil {
		return err
	}
	_, err = db.Exec(update{Model: model})
	return err
}

type update struct {
	*Model
}

func (upd update) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	strct := upd.Value()
	if upd.Table.PK.IsEmpty(strct) {
		return nil, errors.New("primary key is empty")
	}

	b = append(b, "UPDATE "...)
	b = types.AppendField(b, upd.Table.Name, true)
	b = append(b, " SET "...)

	for i, field := range upd.Table.Fields {
		if field.Has(PrimaryKeyFlag) {
			continue
		}
		b = types.AppendField(b, field.SQLName, true)
		b = append(b, " = "...)
		b = field.AppendValue(b, strct, true)
		if i != len(upd.Table.Fields)-1 {
			b = append(b, ", "...)
		}
	}

	b = append(b, " WHERE "...)
	b = types.AppendField(b, upd.Table.PK.SQLName, true)
	b = append(b, " = "...)
	b = upd.Table.PK.AppendValue(b, strct, true)

	return b, nil
}
