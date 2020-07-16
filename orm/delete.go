package orm

type DeleteQuery struct {
	q           *Query
	placeholder bool
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
		q:           q.q.Clone(),
		placeholder: q.placeholder,
	}
}

func (q *DeleteQuery) Query() *Query {
	return q.q
}

func (q *DeleteQuery) AppendTemplate(b []byte) ([]byte, error) {
	cp := q.Clone().(*DeleteQuery)
	cp.placeholder = true
	return cp.AppendQuery(dummyFormatter{}, b)
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

	b = append(b, " WHERE "...)
	value := q.q.tableModel.Value()

	if q.q.isSliceModelWithData() {
		if len(q.q.where) > 0 {
			b, err = q.q.appendWhere(fmter, b)
			if err != nil {
				return nil, err
			}
		} else {
			table := q.q.tableModel.Table()
			err = table.checkPKs()
			if err != nil {
				return nil, err
			}

			b = appendColumnAndSliceValue(fmter, b, value, table.Alias, table.PKs)
		}
	} else {
		b, err = q.q.mustAppendWhere(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	if len(q.q.returning) > 0 {
		b, err = q.q.appendReturning(fmter, b)
		if err != nil {
			return nil, err
		}
	}

	return b, q.q.stickyErr
}
