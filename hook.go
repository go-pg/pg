package pg

import (
	"context"
	"fmt"

	"github.com/go-pg/pg/orm"
)

type dummyFormatter struct{}

func (dummyFormatter) FormatQuery(b []byte, query string, params ...interface{}) []byte {
	return append(b, query...)
}

type QueryEvent struct {
	Ctx     context.Context
	DB      orm.DB
	Query   interface{}
	Params  []interface{}
	Attempt int
	Result  Result
	Error   error

	Data map[interface{}]interface{}
}

type QueryHook interface {
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
	case orm.TemplateAppender:
		return query.AppendTemplate(nil)
	case string:
		return dummyFormatter{}.FormatQuery(nil, query), nil
	default:
		return nil, fmt.Errorf("pg: can't append %T", query)
	}
}

// AddQueryHook adds a hook into query processing.
func (db *baseDB) AddQueryHook(hook QueryHook) {
	db.queryHooks = append(db.queryHooks, hook)
}

func (db *baseDB) queryStarted(
	c context.Context,
	ormDB orm.DB,
	query interface{},
	params []interface{},
	attempt int,
) *QueryEvent {
	if len(db.queryHooks) == 0 {
		return nil
	}

	event := &QueryEvent{
		Ctx:     c,
		DB:      ormDB,
		Query:   query,
		Params:  params,
		Attempt: attempt,
		Data:    make(map[interface{}]interface{}),
	}
	for _, hook := range db.queryHooks {
		hook.BeforeQuery(event)
	}
	return event
}

func (db *baseDB) queryProcessed(
	res Result,
	err error,
	event *QueryEvent,
) {
	if event == nil {
		return
	}

	event.Error = err
	event.Result = res
	for _, hook := range db.queryHooks {
		hook.AfterQuery(event)
	}
}

func copyQueryHooks(s []QueryHook) []QueryHook {
	return s[:len(s):len(s)]
}
