package orm

import (
	"errors"
	"fmt"
	"strings"
)

func HasTable(db DB, model interface{}) (bool, error) {
	return NewQuery(db, model).HasTable()
}

type hasTableQuery struct {
	q *Query
}

func (q hasTableQuery) Copy() QueryAppender {
	return q
}

func (q hasTableQuery) Query() *Query {
	return q.q
}

func (q hasTableQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}
	if q.q.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}


	b = append(b, fmt.Sprintf(
		"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = %s);", strings.Replace(
			string(q.q.model.Table().Name), `"`, "'", -1),
	)...)

	return b, nil
}
