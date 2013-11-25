package pg_test

import (
	"database/sql"
	"net"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	. "launchpad.net/gocheck"

	"github.com/vmihailenco/pg"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&PoolTest{})

type PoolTest struct {
	db *pg.DB
}

func (t *PoolTest) SetUpTest(c *C) {
	t.db = pg.Connect(&pg.Options{
		User:     "test",
		Database: "test",
		PoolSize: 10,

		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
}

func (t *PoolTest) TearDownTest(c *C) {
	t.db.Close()
}

func (t *PoolTest) TestPoolReusesConnection(c *C) {
	for i := 0; i < 100; i++ {
		_, err := t.db.Exec("SELECT 1")
		c.Assert(err, IsNil)
	}

	c.Assert(t.db.Pool().Size(), Equals, 1)
	c.Assert(t.db.Pool().Len(), Equals, 1)
}

func (t *PoolTest) TestPoolMaxSize(c *C) {
	N := 1000

	wg := &sync.WaitGroup{}
	wg.Add(N)
	for i := 0; i < 1000; i++ {
		go func() {
			_, err := t.db.Exec("SELECT 1")
			c.Assert(err, IsNil)
			wg.Done()
		}()
	}
	wg.Wait()

	c.Assert(t.db.Pool().Size(), Equals, 10)
	c.Assert(t.db.Pool().Len(), Equals, 10)
}

func (t *PoolTest) TestTimeoutAndCancelRequest(c *C) {
	_, err := t.db.Exec("SELECT pg_sleep(60)")
	c.Assert(err.(net.Error).Timeout(), Equals, true)

	c.Assert(t.db.Pool().Size(), Equals, 0)
	c.Assert(t.db.Pool().Len(), Equals, 0)

	// Unreliable check that previous query was cancelled.
	var count int
	_, err = t.db.QueryOne(pg.LoadInto(&count), "SELECT COUNT(*) FROM pg_stat_activity")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)
}

func (t *PoolTest) TestCloseClosesAllConnections(c *C) {
	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		_, _, err := ln.ReceiveTimeout(0)
		c.Assert(err, Not(IsNil))
		c.Assert(err.Error(), Equals, "read tcp 127.0.0.1:5432: use of closed network connection")
		close(done)
	}()

	<-started
	c.Assert(t.db.Close(), IsNil)
	<-done
}

func (t *PoolTest) TestClosedDB(c *C) {
	c.Assert(t.db.Close(), IsNil)

	err := t.db.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: database is closed")

	_, err = t.db.Exec("SELECT 1")
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: database is closed")
}

func (t *PoolTest) TestClosedListener(c *C) {
	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)

	c.Assert(ln.Close(), IsNil)

	err = ln.Close()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: listener is closed")

	_, _, err = ln.Receive()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: listener is closed")
}

func (t *PoolTest) TestClosedStatement(c *C) {
	tx, err := t.db.Begin()
	c.Assert(err, IsNil)

	c.Assert(tx.Rollback(), IsNil)

	err = tx.Rollback()
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: transaction has already been committed or rolled back")

	_, err = tx.Exec("SELECT 1")
	c.Assert(err, Not(IsNil))
	c.Assert(err.Error(), Equals, "pg: transaction has already been committed or rolled back")
}

var _ = Suite(&DBTest{})

type DBTest struct {
	db            *pg.DB
	pqdb, mysqldb *sql.DB
}

func (t *DBTest) SetUpTest(c *C) {
	t.db = pg.Connect(&pg.Options{
		User:     "test",
		Database: "test",
		PoolSize: 2,

		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})

	pqdb, err := sql.Open("postgres", "user=test dbname=test")
	c.Assert(err, IsNil)
	t.pqdb = pqdb

	mysqldb, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/test")
	c.Assert(err, IsNil)
	t.mysqldb = mysqldb
}

func (t *DBTest) TestFormatWithTooManyParams(c *C) {
	q, err := pg.FormatQ("", "foo", "bar")
	c.Assert(err.Error(), Equals, "pg: expected 0 parameters but got 2")
	c.Assert(string(q), Equals, "")
}

func (t *DBTest) TestFormatWithTooFewParams(c *C) {
	q, err := pg.FormatQ("? ? ?", "foo", "bar")
	c.Assert(err.Error(), Equals, "pg: expected at least 3 parameters but got 2")
	c.Assert(string(q), Equals, "")
}

func (t *DBTest) TestFormatInts(c *C) {
	q, err := pg.FormatQ("?", pg.Ints{1, 2, 3})
	c.Assert(err, IsNil)
	c.Assert(q, Equals, pg.Q("1,2,3"))
}

func (t *DBTest) TestFormatStrings(c *C) {
	q, err := pg.FormatQ("?", pg.Strings{"hello", "world"})
	c.Assert(err, IsNil)
	c.Assert(q, Equals, pg.Q("'hello','world'"))
}

type Dst struct {
	Num int
}

func (d *Dst) New() interface{} {
	return d
}

func (t *DBTest) TestQuery(c *C) {
	dst := &Dst{}
	res, err := t.db.Query(dst, "SELECT 1 AS num")
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 1)
	c.Assert(res.Affected(), Equals, 1)
}

func (t *DBTest) TestQueryZeroRows(c *C) {
	res, err := t.db.Query(&Dst{}, "SELECT s.num AS num FROM generate_series(0, -1) AS s(num)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 0)
}

func (t *DBTest) TestQueryOne(c *C) {
	dst := &Dst{}
	res, err := t.db.QueryOne(dst, "SELECT 1 AS num")
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 1)
	c.Assert(res.Affected(), Equals, 1)
}

func (t *DBTest) TestQueryOneValue(c *C) {
	var v int
	_, err := t.db.QueryOne(pg.LoadInto(&v), "SELECT 1 AS num")
	c.Assert(err, IsNil)
	c.Assert(v, Equals, 1)
}

func (t *DBTest) TestQueryOneErrNoRows(c *C) {
	dst, err := t.db.QueryOne(&Dst{}, "SELECT s.num AS num FROM generate_series(0, -1) AS s(num)")
	c.Assert(dst, IsNil)
	c.Assert(err, Equals, pg.ErrNoRows)
}

func (t *DBTest) TestQueryOneErrMultiRows(c *C) {
	dst, err := t.db.QueryOne(&Dst{}, "SELECT s.num AS num FROM generate_series(0, 10) AS s(num)")
	c.Assert(err, Equals, pg.ErrMultiRows)
	c.Assert(dst, IsNil)
}

func (t *DBTest) TestTypeString(c *C) {
	src := "hello\000"
	var dst string
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?", src)
	c.Assert(err, IsNil)
	c.Assert(dst, Equals, "hello")
}

func (t *DBTest) TestTypeStmtString(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::text")
	c.Assert(err, IsNil)

	src := "hello\000"
	var dst string
	_, err = stmt.QueryOne(pg.LoadInto(&dst), src)
	c.Assert(err, IsNil)
	c.Assert(dst, Equals, "hello")
}

func (t *DBTest) TestTypeBytes(c *C) {
	src := []byte("hello world\000")
	var dst []byte
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?::bytea", src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *DBTest) TestTypeStmtBytes(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::bytea")
	c.Assert(err, IsNil)

	src := []byte("hello world\000")
	var dst []byte
	_, err = stmt.QueryOne(pg.LoadInto(&dst), src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)

	c.Assert(stmt.Close(), IsNil)
}

func (t *DBTest) TestTypeDate(c *C) {
	src := time.Now().UTC()
	var dst time.Time
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?::date", src)
	c.Assert(err, IsNil)
	c.Assert(dst.Location(), Equals, time.UTC)
	c.Assert(dst.Format("2006-01-02"), Equals, dst.Format("2006-01-02"))
}

func (t *DBTest) TestTypeTime(c *C) {
	src := time.Now().UTC()
	var dst time.Time
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?::time", src)
	c.Assert(err, IsNil)
	c.Assert(dst.Location(), Equals, time.UTC)
	c.Assert(
		dst.Format("15:04:05.9999"),
		Equals,
		src.Format("15:04:05.9999"),
	)
}

func (t *DBTest) TestTypeTimestamp(c *C) {
	src := time.Now().UTC()
	var dst time.Time
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?::timestamp", src)
	c.Assert(err, IsNil)
	c.Assert(dst.Location(), Equals, time.UTC)
	c.Assert(
		dst.Format("2006-01-02 15:04:05.9999"),
		Equals,
		src.Format("2006-01-02 15:04:05.9999"),
	)
}

func (t *DBTest) TestTypeStringArray(c *C) {
	src := []string{"foo \n", "bar", "hello {}", "'\\\""}
	var dst []string
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?::text[]", src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *DBTest) TestTypeStmtStringArray(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::text[]")
	c.Assert(err, IsNil)

	src := []string{"foo \n", "bar", "hello {}", "'\\\""}
	var dst []string
	_, err = stmt.QueryOne(pg.LoadInto(&dst), src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *DBTest) TestTypeEmptyStringArray(c *C) {
	var dst []string
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?::text[]", []string{})
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, []string{})
}

func (t *DBTest) TestTypeStmtEmptyStringArray(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::text[]")
	c.Assert(err, IsNil)

	var dst []string
	_, err = stmt.QueryOne(pg.LoadInto(&dst), []string{})
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, []string{})
}

func (t *DBTest) TestTypeIntArray(c *C) {
	src := []int{1, 2, 3}
	var dst []int
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?::int[]", []int{1, 2, 3})
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *DBTest) TestTypeStmtIntArray(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::int[]")
	c.Assert(err, IsNil)

	src := []int{1, 2, 3}
	var dst []int
	_, err = stmt.QueryOne(pg.LoadInto(&dst), src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *DBTest) TestTypeEmptyIntArray(c *C) {
	var dst []int
	_, err := t.db.QueryOne(
		pg.LoadInto(&dst),
		"SELECT ?::int[]",
		[]int{},
	)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, []int{})
}

func (t *DBTest) TestTypeHstore(c *C) {
	src := map[string]string{"foo =>": "bar =>", "hello": "world", "'\\\"": "'\\\""}
	dst := make(map[string]string)
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?", src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *DBTest) TestTypeStmtHstore(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::hstore")
	c.Assert(err, IsNil)

	src := map[string]string{"foo =>": "bar =>", "hello": "world", "'\\\"": "'\\\""}
	dst := make(map[string]string)
	_, err = stmt.QueryOne(pg.LoadInto(&dst), src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *DBTest) TestQueryInts(c *C) {
	var ids pg.Ints
	_, err := t.db.Query(&ids, "SELECT s.num AS num FROM generate_series(0, 10) AS s(num)")
	c.Assert(err, IsNil)
	c.Assert(ids, DeepEquals, pg.Ints{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
}

func (t *DBTest) TestQueryInts2(c *C) {
	var ints pg.Ints
	_, err := t.db.Query(&ints, "SELECT * FROM generate_series(1, 1000000)")
	c.Assert(err, IsNil)
	c.Assert(ints, HasLen, 1000000)
}

func (t *DBTest) TestQueryStrings(c *C) {
	var strings pg.Strings
	_, err := t.db.Query(&strings, "SELECT 'hello'")
	c.Assert(err, IsNil)
	c.Assert(strings, DeepEquals, pg.Strings{"hello"})
}

func (t *DBTest) TestExec(c *C) {
	res, err := t.db.Exec("CREATE TEMP TABLE test(id serial PRIMARY KEY)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 0)

	res, err = t.db.Exec("INSERT INTO test VALUES (1)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 1)
}

func (t *DBTest) TestStatementExec(c *C) {
	res, err := t.db.Exec("CREATE TEMP TABLE test(id serial PRIMARY KEY)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 0)

	stmt, err := t.db.Prepare("INSERT INTO test VALUES($1)")
	c.Assert(err, IsNil)
	defer stmt.Close()

	res, err = stmt.Exec(1)
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 1)
}

func (t *DBTest) TestQueryStmt(c *C) {
	stmt, err := t.db.Prepare("SELECT 1 AS num")
	c.Assert(err, IsNil)
	defer stmt.Close()

	dst := &Dst{}
	res, err := stmt.Query(dst)
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 1)
	c.Assert(res.Affected(), Equals, 1)
}

func (t *DBTest) TestListenNotify(c *C) {
	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)

	_, err = t.db.Exec("NOTIFY test_channel")
	c.Assert(err, IsNil)

	channel, payload, err := ln.Receive()
	c.Assert(err, IsNil)
	c.Assert(channel, Equals, "test_channel")
	c.Assert(payload, Equals, "")

	done := make(chan struct{})
	go func() {
		_, _, err := ln.Receive()
		c.Assert(err.Error(), Equals, "read tcp 127.0.0.1:5432: use of closed network connection")
		done <- struct{}{}
	}()

	select {
	case <-done:
		c.Fail()
	case <-time.After(2 * time.Second):
		// ok
	}

	c.Assert(ln.Close(), IsNil)
	<-done
}

func (t *DBTest) TestListenTimeout(c *C) {
	ln, err := t.db.Listen("test_channel")
	c.Assert(err, IsNil)
	defer ln.Close()

	channel, payload, err := ln.ReceiveTimeout(time.Second)
	c.Assert(err.(net.Error).Timeout(), Equals, true)
	c.Assert(channel, Equals, "")
	c.Assert(payload, Equals, "")
}

func (t *DBTest) BenchmarkFormatWithoutArgs(c *C) {
	for i := 0; i < c.N; i++ {
		_, err := pg.FormatQ("SELECT 'hello', 'world' WHERE 1=1 AND 2=2")
		if err != nil {
			panic(err)
		}
	}
}

func (t *DBTest) BenchmarkFormatWithArgs(c *C) {
	for i := 0; i < c.N; i++ {
		_, err := pg.FormatQ("SELECT ?, ? WHERE 1=1 AND 2=2", "hello", "world")
		if err != nil {
			panic(err)
		}
	}
}

func (t *DBTest) BenchmarkQueryRow(c *C) {
	dst := &Dst{}
	for i := 0; i < c.N; i++ {
		_, err := t.db.QueryOne(dst, "SELECT ?::bigint AS num", 1)
		if err != nil {
			panic(err)
		}
		if dst.Num != 1 {
			panic("dst.Num != 1")
		}
	}
}

func (t *DBTest) BenchmarkQueryRowStdlibPq(c *C) {
	var n int64
	for i := 0; i < c.N; i++ {
		r := t.pqdb.QueryRow("SELECT $1::bigint AS num", 1)
		if err := r.Scan(&n); err != nil {
			panic(err)
		}
		if n != 1 {
			panic("n != 1")
		}
	}
}

func (t *DBTest) BenchmarkQueryRowStdlibMySQL(c *C) {
	var n int64
	for i := 0; i < c.N; i++ {
		r := t.mysqldb.QueryRow("SELECT ? AS num", 1)
		if err := r.Scan(&n); err != nil {
			panic(err)
		}
		if n != 1 {
			panic("n != 1")
		}
	}
}

func (t *DBTest) BenchmarkQueryRowStmt(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::bigint AS num")
	c.Assert(err, IsNil)
	defer stmt.Close()

	for i := 0; i < c.N; i++ {
		_, err := stmt.QueryOne(&Dst{}, 1)
		if err != nil {
			panic(err)
		}
	}
}

func (t *DBTest) BenchmarkQueryRowStmtStdlibPq(c *C) {
	stmt, err := t.pqdb.Prepare("SELECT $1::bigint AS num")
	c.Assert(err, IsNil)
	defer stmt.Close()

	var n int64
	for i := 0; i < c.N; i++ {
		r := stmt.QueryRow(1)
		if err := r.Scan(&n); err != nil {
			panic(err)
		}
	}
}

func (t *DBTest) BenchmarkExec(c *C) {
	_, err := t.db.Exec(
		"CREATE TEMP TABLE exec_test(id bigint, name varchar(500))")
	if err != nil {
		panic(err)
	}

	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		res, err := t.db.Exec("INSERT INTO exec_test(id, name) VALUES(?, ?)", 1, "hello world")
		if err != nil {
			panic(err)
		}
		if res.Affected() != 1 {
			panic("res.Affected() != 1")
		}
	}
}

func (t *DBTest) BenchmarkExecWithError(c *C) {
	_, err := t.db.Exec(
		"CREATE TEMP TABLE exec_with_error_test(id bigint PRIMARY KEY, name varchar(500))")
	if err != nil {
		panic(err)
	}

	_, err = t.db.Exec(
		"INSERT INTO exec_with_error_test(id, name) VALUES(?, ?)",
		1, "hello world",
	)
	if err != nil {
		panic(err)
	}

	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		_, err := t.db.Exec("INSERT INTO exec_with_error_test(id) VALUES(?)", 1)
		if err == nil {
			panic("got nil error, expected IntegrityError")
		} else if _, ok := err.(*pg.IntegrityError); !ok {
			panic("got " + err.Error() + ", expected IntegrityError")
		}
	}
}

func (t *DBTest) BenchmarkExecStmt(c *C) {
	_, err := t.db.Exec("CREATE TEMP TABLE statement_exec(id bigint, name varchar(500))")
	if err != nil {
		panic(err)
	}

	stmt, err := t.db.Prepare("INSERT INTO statement_exec(id, name) VALUES($1, $2)")
	c.Assert(err, IsNil)
	defer stmt.Close()

	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		_, err = stmt.Exec(1, "hello world")
		if err != nil {
			panic(err)
		}
	}
}

func (t *DBTest) BenchmarkExecStmtStdlibPq(c *C) {
	_, err := t.pqdb.Exec("CREATE TEMP TABLE statement_exec(id bigint, name varchar(500))")
	if err != nil {
		panic(err)
	}

	stmt, err := t.pqdb.Prepare("INSERT INTO statement_exec(id, name) VALUES($1, $2)")
	c.Assert(err, IsNil)
	defer stmt.Close()

	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		_, err = stmt.Exec(1, "hello world")
		if err != nil {
			panic(err)
		}
	}
}
