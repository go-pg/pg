package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/go-pg/pg/orm"
)

type dummyFormatter struct{}

func (dummyFormatter) FormatQuery(b []byte, query string, params ...interface{}) []byte {
	return append(b, query...)
}

// QueryEvent ...
type QueryEvent struct {
	StartTime time.Time
	DB        orm.DB
	Query     interface{}
	Params    []interface{}
	Attempt   int
	Result    Result
	Error     error

	Stash map[interface{}]interface{}
}

// QueryHook ...
type QueryHook interface {
	BeforeQuery(context.Context, *QueryEvent) (context.Context, error)
	AfterQuery(context.Context, *QueryEvent) (context.Context, error)
}

// UnformattedQuery returns the unformatted query of a query event
func (ev *QueryEvent) UnformattedQuery() (string, error) {
	b, err := queryString(ev.Query)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// FormattedQuery returns the formatted query of a query event
func (ev *QueryEvent) FormattedQuery() (string, error) {
	b, err := appendQuery(ev.DB, nil, ev.Query, ev.Params...)
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

func (db *baseDB) beforeQuery(
	c context.Context,
	ormDB orm.DB,
	query interface{},
	params []interface{},
	attempt int,
) (context.Context, *QueryEvent, error) {
	if len(db.queryHooks) == 0 {
		return c, nil, nil
	}

	event := &QueryEvent{
		StartTime: time.Now(),
		DB:        ormDB,
		Query:     query,
		Params:    params,
		Attempt:   attempt,
	}
	for _, hook := range db.queryHooks {
		var err error
		c, err = hook.BeforeQuery(c, event)
		if err != nil {
			return nil, nil, err
		}
	}
	return c, event, nil
}

func (db *baseDB) afterQuery(
	c context.Context,
	event *QueryEvent,
	res Result,
	err error,
) error {
	if event == nil {
		return nil
	}

	event.Error = err
	event.Result = res
	for _, hook := range db.queryHooks {
		_, err := hook.AfterQuery(c, event)
		if err != nil {
			return err
		}
	}
	return nil
}

func copyQueryHooks(s []QueryHook) []QueryHook {
	return s[:len(s):len(s)]
}
