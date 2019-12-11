package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/go-pg/pg/v9/orm"
)

type BeforeScanHook = orm.BeforeScanHook
type AfterScanHook = orm.AfterScanHook
type AfterSelectHook = orm.AfterSelectHook
type BeforeInsertHook = orm.BeforeInsertHook
type AfterInsertHook = orm.AfterInsertHook
type BeforeUpdateHook = orm.BeforeUpdateHook
type AfterUpdateHook = orm.AfterUpdateHook
type BeforeDeleteHook = orm.BeforeDeleteHook
type AfterDeleteHook = orm.AfterDeleteHook

//------------------------------------------------------------------------------

type dummyFormatter struct{}

func (dummyFormatter) FormatQuery(b []byte, query string, params ...interface{}) []byte {
	return append(b, query...)
}

// QueryEvent ...
type QueryEvent struct {
	StartTime time.Time
	DB        orm.DB
	Model     interface{}
	Query     interface{}
	Params    []interface{}
	Result    Result
	Err       error

	Stash map[interface{}]interface{}
}

// QueryHook ...
type QueryHook interface {
	BeforeQuery(context.Context, *QueryEvent) (context.Context, error)
	AfterQuery(context.Context, *QueryEvent) error
}

// UnformattedQuery returns the unformatted query of a query event
func (ev *QueryEvent) UnformattedQuery() (string, error) {
	b, err := queryString(ev.Query)
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

// FormattedQuery returns the formatted query of a query event
func (ev *QueryEvent) FormattedQuery() (string, error) {
	b, err := appendQuery(ev.DB.Formatter(), nil, ev.Query, ev.Params...)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// AddQueryHook adds a hook into query processing.
func (db *baseDB) AddQueryHook(hook QueryHook) {
	db.queryHooks = append(db.queryHooks, hook)
}

func (db *baseDB) beforeQuery(
	c context.Context,
	ormDB orm.DB,
	model, query interface{},
	params []interface{},
) (context.Context, *QueryEvent, error) {
	if len(db.queryHooks) == 0 {
		return c, nil, nil
	}

	event := &QueryEvent{
		StartTime: time.Now(),
		DB:        ormDB,
		Model:     model,
		Query:     query,
		Params:    params,
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

	event.Err = err
	event.Result = res

	for _, hook := range db.queryHooks {
		err := hook.AfterQuery(c, event)
		if err != nil {
			return err
		}
	}

	return nil
}

func copyQueryHooks(s []QueryHook) []QueryHook {
	return s[:len(s):len(s)]
}
