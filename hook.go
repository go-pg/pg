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

type QueryEvent struct {
	StartTime time.Time
	Func      string
	File      string
	Line      int

	DB      orm.DB
	Query   interface{}
	Params  []interface{}
	Attempt int
	Result  orm.Result
	Error   error

	Data map[interface{}]interface{}
}

type Hook interface {
	BeforeQuery(*QueryEvent)
	AfterQuery(*QueryEvent)
}

func (ev *QueryEvent) UnformattedQuery() (string, error) {
	b, err := queryString(ev.Query)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (ev *QueryEvent) FormattedQuery() (string, error) {
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

// OnQueryEvent calls the interface with QueryEvent
// when query is processed.
func (db *DB) OnQueryEvent(hook Hook) {
	db.queryEventHooks = append(db.queryEventHooks, hook)
}

func (db *DB) queryStarted(
	ormDB orm.DB,
	query interface{},
	params []interface{},
	attempt int,
) *QueryEvent {
	if len(db.queryEventHooks) == 0 {
		return nil
	}

	event := &QueryEvent{
		DB:      ormDB,
		Query:   query,
		Params:  params,
		Attempt: attempt,
		Data: make(map[interface{}]interface{}),
	}
	for _, hook := range db.queryEventHooks {
		hook.BeforeQuery(event)
	}
	return event
}

func (db *DB) queryProcessed(
	res orm.Result,
	err error,
	event *QueryEvent,
) {
	if event == nil {
		return
	}

	event.Error = err
	event.Result = res
	for _, hook := range db.queryEventHooks {
		hook.AfterQuery(event)
	}
}

func copyQueryEventHooks(s []Hook) []Hook {
	return s[:len(s):len(s)]
}
