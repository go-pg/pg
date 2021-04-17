package orm

type DeleteQuery struct {
	q *Query
}

var (
	_ QueryAppender = (*DeleteQuery)(nil)
	_ QueryCommand  = (*DeleteQuery)(nil)
)

func NewDeleteQuery(q *Query) *DeleteQuery {
	return &DeleteQuery{
		q: q,
	}
}

func (q *DeleteQuery) String() string {
	b, err := q.AppendQuery(defaultFmter, nil)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (q *DeleteQuery) Operation() QueryOp {
	return DeleteOp
}

func (q *DeleteQuery) Clone() QueryCommand {
	return &DeleteQuery{
		q: q.q.Clone(),
	}
}

func (q *DeleteQuery) Query() *Query {
	return q.q
}

func (q *DeleteQuery) AppendTemplate(b []byte) ([]byte, error) {
	return q.AppendQuery(dummyFormatter{}, b)
}

func (q *DeleteQuery) AppendQuery(fmter QueryFormatter, b []byte) (_ []byte, err error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}

	if len(q.q.with) > 0 {
		b, err = q.q.appendWith(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	b = append(b, "DELETE FROM "...)
	b, err = q.q.appendFirstTableWithAlias(fmter, b)
	if err != nil {
		return nil, err
	}

	if q.q.hasMultiTables() {
		b = append(b, " USING "...)
		b, err = q.q.appendOtherTables(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	if q.q.isSliceModelWithData() {
		b = append(b, " USING "...)
		b, err = q.q.mustAppendSliceValues(fmter, b, false)
		if err != nil {
			return nil, err
		}
	}

	b, err = q.q.mustAppendWhere(fmter, b)
	if err != nil {
		return nil, err
	}

	if len(q.q.returning) > 0 {
		b, err = q.q.appendReturning(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, q.q.stickyErr
}
