package pg

import (
	"fmt"
	"time"

	"github.com/go-pg/pg/orm"
)

type dummyDB struct {
	orm.DB
}

var _ orm.DB = dummyDB{}

func (dummyDB) FormatQuery(dst []byte, query string, params ...interface{}) []byte {
	return append(dst, query...)
}

type QueryProcessedEvent struct {
	DB        orm.DB
	StartTime time.Time
	Query     interface{}
	Params    []interface{}
	Result    orm.Result
	Error     error
}

func (ev *QueryProcessedEvent) UnformattedQuery() (string, error) {
	b, err := queryString(ev.Query)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (ev *QueryProcessedEvent) FormattedQuery() (string, error) {
	b, err := appendQuery(nil, ev.DB, ev.Query, ev.Params...)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func queryString(query interface{}) ([]byte, error) {
	switch query := query.(type) {
	case orm.QueryAppender:
		query = query.Copy()
		query.Query().DB(dummyDB{})
		return query.AppendQuery(nil)
	case string:
		return dummyDB{}.FormatQuery(nil, query), nil
	default:
		return nil, fmt.Errorf("pg: can't append %T", query)
	}
}

type queryProcessedHook func(*QueryProcessedEvent)

// OnQueryProcessed calls the fn with QueryProcessedEvent
// when query is processed.
func (db *DB) OnQueryProcessed(fn func(*QueryProcessedEvent)) {
	db.queryProcessedHooks = append(db.queryProcessedHooks, fn)
}

func (db *DB) queryProcessed(
	ormDB orm.DB,
	start time.Time,
	query interface{},
	params []interface{},
	res orm.Result,
	err error,
) {
	if len(db.queryProcessedHooks) == 0 {
		return
	}

	event := &QueryProcessedEvent{
		DB:        ormDB,
		StartTime: start,
		Query:     query,
		Params:    params,
		Result:    res,
		Error:     err,
	}
	for _, hook := range db.queryProcessedHooks {
		hook(event)
	}
}
