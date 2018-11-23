package pg

import (
	"fmt"
	"runtime"
	"strings"
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
}

type QueryStartedEvent struct {
	Func string
	File string
	Line int

	DB      orm.DB
	Query   interface{}
	Params  []interface{}
	Attempt int
}

func unformattedQuery(query interface{}) (string, error) {
	b, err := queryString(query)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (ev *QueryProcessedEvent) UnformattedQuery() (string, error) {
	return unformattedQuery(ev.Query)
}

func (ev *QueryStartedEvent) UnformattedQuery() (string, error) {
	return unformattedQuery(ev.Query)
}

func formattedQuery(fmter orm.QueryFormatter, query interface{}, params ...interface{}) (string, error) {
	b, err := appendQuery(nil, fmter, query, params)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (ev *QueryProcessedEvent) FormattedQuery() (string, error) {
	return formattedQuery(ev.DB, ev.Query, ev.Params...)
}

func (ev *QueryStartedEvent) FormattedQuery() (string, error) {
	return formattedQuery(ev.DB, ev.Query, ev.Params...)
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
type queryStartedHook func(*QueryStartedEvent)

// OnQueryProcessed calls the fn with QueryProcessedEvent
// when query is processed.
func (db *DB) OnQueryProcessed(fn func(*QueryProcessedEvent)) {
	db.queryProcessedHooks = append(db.queryProcessedHooks, fn)
}

// OnQueryStarted calls the fn with QueryStartedEvent
// before query is started.
func (db *DB) OnQueryStarted(fn func(*QueryStartedEvent)) {
	db.queryStartedHooks = append(db.queryStartedHooks, fn)
}

func (db *DB) queryStarted(
	ormDB orm.DB,
	query interface{},
	params []interface{},
	attempt int,
) {
	if len(db.queryStartedHooks) == 0 {
		return
	}

	funcName, file, line := fileLine(2)
	event := &QueryStartedEvent{
		Func: funcName,
		File: file,
		Line: line,

		DB:      ormDB,
		Query:   query,
		Params:  params,
		Attempt: attempt,
	}
	for _, hook := range db.queryStartedHooks {
		hook(event)
	}
}

func (db *DB) queryProcessed(
	ormDB orm.DB,
	start time.Time,
	query interface{},
	params []interface{},
	attempt int,
	res orm.Result,
	err error,
) {
	if len(db.queryProcessedHooks) == 0 {
		return
	}

	funcName, file, line := fileLine(2)
	event := &QueryProcessedEvent{
		StartTime: start,
		Func:      funcName,
		File:      file,
		Line:      line,

		DB:      ormDB,
		Query:   query,
		Params:  params,
		Attempt: attempt,
		Result:  res,
		Error:   err,
	}
	for _, hook := range db.queryProcessedHooks {
		hook(event)
	}
}

const packageName = "github.com/go-pg/pg"

func fileLine(depth int) (string, string, int) {
	for i := depth; ; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if strings.Contains(file, packageName) {
			continue
		}
		_, funcName := packageFuncName(pc)
		return funcName, file, line
	}
	return "", "", 0
}

func packageFuncName(pc uintptr) (string, string) {
	f := runtime.FuncForPC(pc)
	if f == nil {
		return "", ""
	}

	packageName := ""
	funcName := f.Name()

	if ind := strings.LastIndex(funcName, "/"); ind > 0 {
		packageName += funcName[:ind+1]
		funcName = funcName[ind+1:]
	}
	if ind := strings.Index(funcName, "."); ind > 0 {
		packageName += funcName[:ind]
		funcName = funcName[ind+1:]
	}

	return packageName, funcName
}

func copyQueryProcessedHooks(s []queryProcessedHook) []queryProcessedHook {
	return s[:len(s):len(s)]
}

func copyQueryStartedHooks(s []queryStartedHook) []queryStartedHook {
	return s[:len(s):len(s)]
}
