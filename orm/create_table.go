package orm

import (
	"fmt"
	"reflect"
)

type CreateTableOptions struct {
	Temp bool
}

func CreateTable(db DB, model interface{}, opt *CreateTableOptions) (Result, error) {
	return db.Exec(createTableQuery{model: model, opt: opt})
}

type createTableQuery struct {
	model interface{}
	opt   *CreateTableOptions
}

func (c createTableQuery) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	typ := reflect.TypeOf(c.model)
	switch typ.Kind() {
	case reflect.Ptr:
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("pg: Model(unsupported %s)", typ)
	}

	table := Tables.Get(typ)

	b = append(b, "CREATE "...)
	if c.opt != nil && c.opt.Temp {
		b = append(b, "TEMP "...)
	}
	b = append(b, "TABLE "...)
	b = append(b, table.Name...)
	b = append(b, " ("...)

	for i, field := range table.Fields {
		b = append(b, field.SQLName...)
		b = append(b, " "...)
		b = append(b, field.SQLType...)
		if field.Has(NotNullFlag) {
			b = append(b, " NOT NULL"...)
		}
		if field.Has(UniqueFlag) {
			b = append(b, " UNIQUE"...)
		}

		if i != len(table.Fields)-1 {
			b = append(b, ", "...)
		}
	}

	b = appendPKConstraint(b, table.PKs)

	b = append(b, ")"...)

	return b, nil
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
