package orm

import (
	"strconv"
)

type CreateCompositeOptions struct {
	Varchar int // replaces PostgreSQL data type `text` with `varchar(n)`
}

func CreateComposite(db DB, model interface{}, opt *CreateCompositeOptions) error {
	q := NewQuery(db, model)
	_, err := q.db.Exec(&createCompositeQuery{
		q:   q,
		opt: opt,
	})
	return err
}

type createCompositeQuery struct {
	q   *Query
	opt *CreateCompositeOptions
}

func (q *createCompositeQuery) Copy() *createCompositeQuery {
	return &createCompositeQuery{
		q:   q.q.Copy(),
		opt: q.opt,
	}
}

func (q *createCompositeQuery) Query() *Query {
	return q.q
}

func (q *createCompositeQuery) AppendTemplate(b []byte) ([]byte, error) {
	cp := q.Copy()
	cp.q = cp.q.Formatter(dummyFormatter{})
	return cp.AppendQuery(b)
}

func (q *createCompositeQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}
	if q.q.model == nil {
		return nil, errModelNil
	}

	table := q.q.model.Table()

	b = append(b, "CREATE TYPE "...)
	b = append(b, q.q.model.Table().Alias...)
	b = append(b, " AS ("...)

	for i, field := range table.Fields {
		if i > 0 {
			b = append(b, ", "...)
		}

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
	}

	b = append(b, ")"...)

	return b, q.q.stickyErr
}
