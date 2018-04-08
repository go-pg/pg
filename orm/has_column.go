package orm

import (
	"errors"
	"strings"

	"github.com/go-pg/pg/types"
)

func HasColumn(db DB, model interface{}, column string) (bool, error) {
	q := NewQuery(db, model)
	var count int
	_, err := q.db.QueryOne(Scan(&count), hasColumnQuery{
		q:      q,
		column: column,
	})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

type hasColumnQuery struct {
	q      *Query
	column string
}

func (q hasColumnQuery) Copy() QueryAppender {
	return q
}

func (q hasColumnQuery) Query() *Query {
	return q.q
}

func (q hasColumnQuery) AppendQuery(b []byte) ([]byte, error) {
	if q.q.stickyErr != nil {
		return nil, q.q.stickyErr
	}
	if q.q.model == nil {
		return nil, errors.New("pg: Model(nil)")
	}

	tableName := string(q.q.appendTableName(nil))
	tableName = strings.TrimPrefix(tableName, `"`)
	tableName = strings.TrimSuffix(tableName, `"`)

	b = append(b, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = "...)
	b = types.AppendString(b, tableName, 1)
	b = append(b, " AND column_name = "...)
	b = types.AppendString(b, q.column, 1)

	return b, nil
}
