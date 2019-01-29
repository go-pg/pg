package orm

type DropCompositeOptions struct {
	IfExists bool
	Cascade  bool
}

func DropComposite(db DB, model interface{}, opt *DropCompositeOptions) error {
	q := NewQuery(db, model)
	_, err := q.db.Exec(&dropCompositeQuery{
		q:   q,
		opt: opt,
	})
	return err
}

type dropCompositeQuery struct {
	q   *Query
	opt *DropCompositeOptions
}

func (q *dropCompositeQuery) Copy() *dropCompositeQuery {
	return &dropCompositeQuery{
		q:   q.q.Copy(),
		opt: q.opt,
	}
}

func (q *dropCompositeQuery) Query() *Query {
	return q.q
}

func (q *dropCompositeQuery) AppendTemplate(b []byte) ([]byte, error) {
	cp := q.Copy()
	cp.q = cp.q.Formatter(dummyFormatter{})
	return cp.AppendQuery(b)
}

func (q *dropCompositeQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}
	if q.q.model == nil {
		return nil, errModelNil
	}

	b = append(b, "DROP TYPE "...)
	if q.opt != nil && q.opt.IfExists {
		b = append(b, "IF EXISTS "...)
	}
	b = append(b, q.q.model.Table().Alias...)
	if q.opt != nil && q.opt.Cascade {
		b = append(b, " CASCADE"...)
	}

	return b, q.q.stickyErr
}
