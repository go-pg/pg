package orm

import "gopkg.in/pg.v3/types"

func Create(query querier, v interface{}) error {
	model, err := NewModel(v)
	if err != nil {
		return err
	}
	return query(model, insert{Model: model})
}

type insert struct {
	*Model
}

func (ins insert) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	strct := ins.Value()

	b = append(b, "INSERT INTO "...)
	b = types.AppendField(b, ins.Table.Name, true)

	b = append(b, " ("...)
	for i, field := range ins.Table.Fields {
		if field.Has(PrimaryKeyFlag) && field.IsEmpty(strct) {
			continue
		}
		b = types.AppendField(b, field.SQLName, true)
		if i != len(ins.Table.Fields)-1 {
			b = append(b, ", "...)
		}
	}
	b = append(b, ") VALUES ("...)

	for i, field := range ins.Table.Fields {
		if field.Has(PrimaryKeyFlag) && field.IsEmpty(strct) {
			continue
		}
		b = field.AppendValue(b, strct, true)
		if i != len(ins.Table.Fields)-1 {
			b = append(b, ", "...)
		}
	}
	b = append(b, ")"...)

	if ins.Table.PK.IsEmpty(strct) {
		b = append(b, " RETURNING "...)
		b = types.AppendField(b, ins.Table.PK.SQLName, true)
	}

	return b, nil
}
