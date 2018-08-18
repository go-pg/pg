package orm

import "errors"

type DropTableOptions struct {
	IfExists bool
	Cascade  bool
}

func DropTable(db DB, model interface{}, opt *DropTableOptions) error {
	q := NewQuery(db, model)
	_, err := q.db.Exec(dropTableQuery{
		q:   q,
		opt: opt,
	})
	return err
}

type dropTableQuery struct {
	q   *Query
	opt *DropTableOptions
}

func (q dropTableQuery) Copy() QueryAppender {
	return q
}

func (q dropTableQuery) Query() *Query {
	return q.q
}

func (q dropTableQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}
	if q.q.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}

	b = append(b, "DROP TABLE "...)
	if q.opt != nil && q.opt.IfExists {
		b = append(b, "IF EXISTS "...)
	}
	b = q.q.appendTableName(b)
	if q.opt != nil && q.opt.Cascade {
		b = append(b, " CASCADE"...)
	}

	return b, nil
}
