package pg

import (
	"time"

	"github.com/go-pg/pg/orm"
)

type QueryProcessedEvent struct {
	StartTime time.Time
	Query     interface{}
	Params    []interface{}
	Result    orm.Result
	Error     error
}

type queryProcessedHook func(orm.DB, *QueryProcessedEvent)

// OnQueryProcessed calls the fn with QueryProcessedEvent
// when query is processed.
func (db *DB) OnQueryProcessed(fn queryProcessedHook) {
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
		StartTime: start,
		Query:     query,
		Params:    params,
		Result:    res,
		Error:     err,
	}
	for _, hook := range db.queryProcessedHooks {
		hook(ormDB, event)
	}
}
