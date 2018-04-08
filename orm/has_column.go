package orm

import (
	"errors"
	"strings"
)

func HasColumn(db DB, model interface{}, clm string) (bool, error) {
	return NewQuery(db, model).HasColumn(clm)
}

type hasColumnQuery struct {
	q       *Query
	clmName string
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

	b = append(b, "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='"...)
	b = append(b, tableName...)
	b = append(b, "' AND column_name='"...)
	b = append(b, q.clmName...)
	b = append(b, "';"...)

	return b, nil
}
