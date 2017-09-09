package orm

import (
	"errors"
	"strconv"
)

type CreateTableOptions struct {
	Temp        bool
	IfNotExists bool
	Varchar     int // replaces PostgreSQL data type `text` with `varchar(n)`
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
		return nil, errors.New("pg: Model is nil")
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
	b = q.q.appendTableName(b)
	b = append(b, " ("...)

	for i, field := range table.Fields {
		b = append(b, field.Column...)
		b = append(b, " "...)
		if q.opt != nil && q.opt.Varchar > 0 &&
			field.SQLType == "text" && !field.HasFlag(customTypeFlag) {
			b = append(b, "varchar("...)
			b = strconv.AppendInt(b, int64(q.opt.Varchar), 10)
			b = append(b, ")"...)
		} else {
			b = append(b, field.SQLType...)
		}
		if field.HasFlag(NotNullFlag) {
			b = append(b, " NOT NULL"...)
		}
		if field.HasFlag(UniqueFlag) {
			b = append(b, " UNIQUE"...)
		}
		if field.Default != "" {
			b = append(b, " DEFAULT "...)
			b = append(b, field.Default...)
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
		b = append(b, pk.Column...)

		if i != len(primaryKeys)-1 {
			b = append(b, ", "...)
		}
	}
	b = append(b, ")"...)
	return b
}
