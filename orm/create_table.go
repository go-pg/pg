package orm

import "errors"

type CreateTableOptions struct {
	Temp        bool
	IfNotExists bool
}

func CreateTable(db DB, model interface{}, opt *CreateTableOptions) (Result, error) {
	return NewQuery(db, model).CreateTable(opt)
}

type createTableQuery struct {
	q   *Query
	opt *CreateTableOptions
}

func (q createTableQuery) Copy() QueryAppender {
	return q
}

func (q createTableQuery) Query() *Query {
	return q.q
}

func (q createTableQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}
	if q.q.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}

	table := q.q.model.Table()

	b = append(b, "CREATE "...)
	if q.opt != nil && q.opt.Temp {
		b = append(b, "TEMP "...)
	}
	b = append(b, "TABLE "...)
	if q.opt != nil && q.opt.IfNotExists {
		b = append(b, "IF NOT EXISTS "...)
	}
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
