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

	funcName, file, line := fileLine(2)
	event := &QueryEvent{
		StartTime: time.Now(),
		Func: funcName,
		File: file,
		Line: line,

		DB:      ormDB,
		Query:   query,
		Params:  params,
		Attempt: attempt,
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
	if len(db.queryEventHooks) == 0 {
		return
	}

	funcName, file, line := fileLine(2)
	event.Func = funcName
	event.File = file
	event.Line = line
	event.Error = err
	event.Result = res
	for _, hook := range db.queryEventHooks {
		hook.AfterQuery(event)
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

func copyQueryEventHooks(s []Hook) []Hook {
	return s[:len(s):len(s)]
}
