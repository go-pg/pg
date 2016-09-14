package orm

import (
	"reflect"

	"gopkg.in/pg.v4/types"
)

type createTableQuery struct {
	model interface{}
}

func (c createTableQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	table := Tables.Get(reflect.TypeOf(c.model))

	b = append(b, "CREATE TABLE "...)
	b = append(b, table.Name...)
	b = append(b, " ("...)

	for i, field := range table.Fields {
		b = append(b, field.SQLName...)
		b = append(b, " "...)
		b = append(b, field.SQLType...)

		if i != len(table.Fields)-1 {
			b = append(b, ", "...)
		}
	}

	b = appendPKConstraint(b, table.PKs)

	b = append(b, ")"...)
	return b, nil
}

func CreateTable(db DB, model interface{}) (*types.Result, error) {
	return db.Exec(createTableQuery{model: model})
}

func appendPKConstraint(b []byte, primaryKeys []*Field) []byte {
	if len(primaryKeys) == 0 {
		return b
	}

	b = append(b, ", PRIMARY KEY ("...)
	for i, pk := range primaryKeys {
		b = append(b, pk.SQLName...)

		if i != len(primaryKeys)-1 {
			b = append(b, ", "...)
		}
	}
	b = append(b, ")"...)
	return b
}
