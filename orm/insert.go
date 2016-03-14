package orm

import "gopkg.in/pg.v4/types"

func Create(db dber, v interface{}) error {
	model, err := NewTableModel(v)
	if err != nil {
		return err
	}
	_, err = db.Query(model, insert{TableModel: model})
	return err
}

type insert struct {
	TableModel
}

func (ins insert) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	table := ins.Table()
	strct := ins.Value()

	b = append(b, "INSERT INTO "...)
	b = types.AppendField(b, table.Name, true)

	b = append(b, " ("...)
	for i, field := range table.Fields {
		if field.Has(PrimaryKeyFlag) && field.IsEmpty(strct) {
			continue
		}
		b = types.AppendField(b, field.SQLName, true)
		if i != len(table.Fields)-1 {
			b = append(b, ", "...)
		}
	}

	b = append(b, ") VALUES ("...)

	for i, field := range table.Fields {
		if field.Has(PrimaryKeyFlag) && field.IsEmpty(strct) {
			continue
		}
		b = field.AppendValue(b, strct, true)
		if i != len(table.Fields)-1 {
			b = append(b, ", "...)
		}
	}
	b = append(b, ")"...)

	b = appendReturning(b, strct, table.PKs)

	return b, nil
}
