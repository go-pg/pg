package orm

import (
	"strconv"
)

type CreateCompositeOptions struct {
	Varchar int // replaces PostgreSQL data type `text` with `varchar(n)`
}

type createCompositeQuery struct {
	q   *Query
	opt *CreateCompositeOptions
}

var (
	_ QueryAppender = (*createCompositeQuery)(nil)
	_ queryCommand  = (*createCompositeQuery)(nil)
)

func (q *createCompositeQuery) Clone() queryCommand {
	return &createCompositeQuery{
		q:   q.q.Clone(),
		opt: q.opt,
	}
}

func (q *createCompositeQuery) Query() *Query {
	return q.q
}

func (q *createCompositeQuery) AppendTemplate(b []byte) ([]byte, error) {
	return q.AppendQuery(dummyFormatter{}, b)
}

func (q *createCompositeQuery) AppendQuery(fmter QueryFormatter, b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}
	if q.q.tableModel == nil {
		return nil, errModelNil
	}

	table := q.q.tableModel.Table()

	b = append(b, "CREATE TYPE "...)
	b = append(b, table.Alias...)
	b = append(b, " AS ("...)

	for i, field := range table.Fields {
		if i > 0 {
			b = append(b, ", "...)
		}

		b = append(b, field.Column...)
		b = append(b, " "...)
		if field.UserSQLType == "" && q.opt != nil && q.opt.Varchar > 0 &&
			field.SQLType == "text" {
			b = append(b, "varchar("...)
			b = strconv.AppendInt(b, int64(q.opt.Varchar), 10)
			b = append(b, ")"...)
		} else {
			b = append(b, field.SQLType...)
		}
	}

	b = append(b, ")"...)

	return b, q.q.stickyErr
}
