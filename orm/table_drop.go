package orm

type DropTableOptions struct {
	IfExists bool
	Cascade  bool
}

func DropTable(db DB, model interface{}, opt *DropTableOptions) error {
	return NewQuery(db, model).DropTable(opt)
}

type dropTableQuery struct {
	q   *Query
	opt *DropTableOptions
}

var _ QueryAppender = (*dropTableQuery)(nil)
var _ queryCommand = (*dropTableQuery)(nil)

func newDropTableQuery(q *Query, opt *DropTableOptions) *dropTableQuery { //nolint:deadcode
	return &dropTableQuery{
		q:   q,
		opt: opt,
	}
}

func (q *dropTableQuery) Clone() queryCommand {
	return &dropTableQuery{
		q:   q.q.Clone(),
		opt: q.opt,
	}
}

func (q *dropTableQuery) Query() *Query {
	return q.q
}

func (q *dropTableQuery) AppendTemplate(b []byte) ([]byte, error) {
	return q.AppendQuery(dummyFormatter{}, b)
}

func (q *dropTableQuery) AppendQuery(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}
	if q.q.model == nil {
		return nil, errModelNil
	}

	b = append(b, "DROP TABLE "...)
	if q.opt != nil && q.opt.IfExists {
		b = append(b, "IF EXISTS "...)
	}
	b, err = q.q.appendFirstTable(fmter, b)
	if err != nil {
		return nil, err
	}
	if q.opt != nil && q.opt.Cascade {
		b = append(b, " CASCADE"...)
	}

	return b, q.q.stickyErr
}
