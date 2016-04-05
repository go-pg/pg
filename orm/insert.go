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

var _ QueryAppender = (*insert)(nil)

func (ins insert) AppendQuery(b []byte, params []interface{}) ([]byte, error) {
	table := ins.Table()
	strct := ins.Value()

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

	b = append(b, ")"...)

	b = appendReturning(b, strct, table.PKs)

	return b, nil
}
