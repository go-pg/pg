package pg_test

import (
	"bytes"
	"database/sql"
	"errors"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	. "launchpad.net/gocheck"

	"github.com/vmihailenco/pg"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&DBTest{})

type DBTest struct {
	db            *pg.DB
	pqdb, mysqldb *sql.DB
}

func (t *DBTest) SetUpTest(c *C) {
	t.db = pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "test",
		PoolSize: 2,

		DialTimeout:  3 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	pqdb, err := sql.Open("postgres", "user=test dbname=test")
	c.Assert(err, IsNil)
	t.pqdb = pqdb

	mysqldb, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/test")
	c.Assert(err, IsNil)
	t.mysqldb = mysqldb
}

func (t *DBTest) TearDownTest(c *C) {
	c.Assert(t.db.Close(), IsNil)
	c.Assert(t.pqdb.Close(), IsNil)
	c.Assert(t.mysqldb.Close(), IsNil)
}

func (t *DBTest) TestFormatWithTooManyParams(c *C) {
	_, err := pg.FormatQ("", "foo", "bar")
	c.Assert(err.Error(), Equals, "pg: expected 0 parameters, got 2")
}

func (t *DBTest) TestFormatWithTooFewParams(c *C) {
	_, err := pg.FormatQ("? ? ?", "foo", "bar")
	c.Assert(err.Error(), Equals, "pg: expected at least 3 parameters, got 2")
}

// TODO: check for overflow?
func (t *DBTest) TestFormatUint64(c *C) {
	q, err := pg.FormatQ("?", uint64(math.MaxUint64))
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "-1")
}

func (t *DBTest) TestFormatInts(c *C) {
	q, err := pg.FormatQ("?", pg.Ints{1, 2, 3})
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "1,2,3")
}

func (t *DBTest) TestFormatStrings(c *C) {
	q, err := pg.FormatQ("?", pg.Strings{"hello", "world"})
	c.Assert(err, IsNil)
	c.Assert(string(q), Equals, "'hello','world'")
}

func (t *DBTest) TestFormatAlias(c *C) {
	{
		type myint int
		q, err := pg.FormatQ("?", myint(42))
		c.Assert(err, IsNil)
		c.Assert(string(q), Equals, "42")
	}

	{
		type mystr string
		q, err := pg.FormatQ("?", mystr("hello world"))
		c.Assert(err, IsNil)
		c.Assert(string(q), Equals, "'hello world'")
	}
}

type structFormatter struct {
	Foo string
}

func (structFormatter) Meth() string {
	return "value"
}

func (structFormatter) MethWithArgs(string) string {
	return "value"
}

func (structFormatter) MethWithReturns() (string, string) {
	return "value1", "value2"
}

type embeddedStructFormatter struct {
	*structFormatter
}

func (embeddedStructFormatter) Meth2() string {
	return "value2"
}

func (t *DBTest) TestFormatStruct(c *C) {
	{
		src := struct{ Foo string }{"bar"}
		q, err := pg.FormatQ("?bar", src)
		c.Assert(err.Error(), Equals, `pg: cannot map "bar"`)
		c.Assert(string(q), Equals, "")
	}

	{
		src := struct{ S1, S2 string }{"value1", "value2"}
		q, err := pg.FormatQ("? ?s1 ? ?s2 ?", "one", "two", "three", src)
		c.Assert(err, IsNil)
		c.Assert(string(q), Equals, "'one' 'value1' 'two' 'value2' 'three'")
	}

	{
		src := &structFormatter{}
		_, err := pg.FormatQ("?MethWithArgs", src)
		c.Assert(err.Error(), Equals, `pg: cannot map "MethWithArgs"`)
	}

	{
		src := &structFormatter{}
		_, err := pg.FormatQ("?MethWithReturns", src)
		c.Assert(err.Error(), Equals, `pg: cannot map "MethWithReturns"`)
	}

	{
		src := &structFormatter{"bar"}
		q, err := pg.FormatQ("?foo ?Meth", src)
		c.Assert(err, IsNil)
		c.Assert(string(q), Equals, "'bar' 'value'")
	}

	{
		src := &embeddedStructFormatter{&structFormatter{"bar"}}
		q, err := pg.FormatQ("?foo ?Meth ?Meth2", src)
		c.Assert(err, IsNil)
		c.Assert(string(q), Equals, "'bar' 'value' 'value2'")
	}
}

type structLoader struct {
	Num int
}

func (l *structLoader) New() interface{} {
	return l
}

type embeddedStructLoader struct {
	*structLoader
	Num2 int
}

func (l *embeddedStructLoader) New() interface{} {
	return l
}

func (t *DBTest) TestQuery(c *C) {
	dst := &structLoader{}
	_, err := t.db.Query(dst, "SELECT 1 AS num")
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 1)
}

func (t *DBTest) TestQueryEmbeddedStruct(c *C) {
	dst := &embeddedStructLoader{
		structLoader: &structLoader{},
	}
	_, err := t.db.Query(dst, "SELECT 1 AS num, 2 as num2")
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 1)
	c.Assert(dst.Num2, Equals, 2)
}

func (t *DBTest) TestQueryZeroRows(c *C) {
	res, err := t.db.Query(&structLoader{}, "SELECT s.num AS num FROM generate_series(0, -1) AS s(num)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 0)
}

func (t *DBTest) TestQueryOneStruct(c *C) {
	dst := &structLoader{}
	res, err := t.db.QueryOne(dst, "SELECT 1 AS num")
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 1)
	c.Assert(res.Affected(), Equals, 1)
}

func (t *DBTest) TestQueryOnePrimitive(c *C) {
	var v int
	_, err := t.db.QueryOne(pg.LoadInto(&v), "SELECT 1 AS num")
	c.Assert(err, IsNil)
	c.Assert(v, Equals, 1)
}

func (t *DBTest) TestQueryOneErrNoRows(c *C) {
	dst, err := t.db.QueryOne(&structLoader{}, "SELECT s.num AS num FROM generate_series(0, -1) AS s(num)")
	c.Assert(dst, IsNil)
	c.Assert(err, Equals, pg.ErrNoRows)
}

func (t *DBTest) TestQueryOneErrMultiRows(c *C) {
	dst, err := t.db.QueryOne(&structLoader{}, "SELECT s.num AS num FROM generate_series(0, 10) AS s(num)")
	c.Assert(err, Equals, pg.ErrMultiRows)
	c.Assert(dst, IsNil)
}

func (t *DBTest) TestExecOne(c *C) {
	_, err := t.db.Exec("CREATE TEMP TABLE test(id int)")
	c.Assert(err, IsNil)

	_, err = t.db.Exec("INSERT INTO test VALUES (1)")
	c.Assert(err, IsNil)

	res, err := t.db.ExecOne("DELETE FROM test WHERE id = 1")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 1)
}

func (t *DBTest) TestExecOneErrNoRows(c *C) {
	_, err := t.db.Exec("CREATE TEMP TABLE test(id int)")
	c.Assert(err, IsNil)

	_, err = t.db.ExecOne("DELETE FROM test WHERE id = 1")
	c.Assert(err, Equals, pg.ErrNoRows)
}

func (t *DBTest) TestExecOneErrMultiRows(c *C) {
	_, err := t.db.Exec("CREATE TEMP TABLE test(id int)")
	c.Assert(err, IsNil)

	_, err = t.db.Exec("INSERT INTO test VALUES (1)")
	c.Assert(err, IsNil)

	_, err = t.db.Exec("INSERT INTO test VALUES (1)")
	c.Assert(err, IsNil)

	_, err = t.db.ExecOne("DELETE FROM test WHERE id = 1")
	c.Assert(err, Equals, pg.ErrMultiRows)
}

func (t *DBTest) TestTypeFloat64(c *C) {
	src := 3.14
	var dst float64
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?", src)
	c.Assert(err, IsNil)
	c.Assert(dst, Equals, src)
}

func (t *DBTest) TestTypeStmtFloat64(c *C) {
	stmt, err := t.db.Prepare("SELECT $1::real")
	c.Assert(err, IsNil)

	src := 3.14
	var dst float64
	_, err = stmt.QueryOne(pg.LoadInto(&dst), src)
	c.Assert(err, IsNil)
	c.Assert(dst, Equals, src)
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
	t.db.Exec("CREATE EXTENSION hstore")

	stmt, err := t.db.Prepare("SELECT $1::hstore")
	c.Assert(err, IsNil)

	src := map[string]string{"foo =>": "bar =>", "hello": "world", "'\\\"": "'\\\""}
	dst := make(map[string]string)
	_, err = stmt.QueryOne(pg.LoadInto(&dst), src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)

	t.db.Exec("DROP EXTENSION hstore")
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

	dst := &structLoader{}
	res, err := stmt.Query(dst)
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 1)
	c.Assert(res.Affected(), Equals, 1)
}

type loader string

func (l loader) Load(colIdx int, colName string, b []byte) error {
	return errors.New(string(l))
}

func (t *DBTest) TestLoaderError(c *C) {
	tx, err := t.db.Begin()
	c.Assert(err, IsNil)

	{
		loader := loader("my error")
		_, err := tx.QueryOne(loader, "SELECT 1, 2")
		c.Assert(err.Error(), Equals, "my error")
	}

	{
		var n1, n2 int
		_, err := tx.QueryOne(pg.LoadInto(&n1, &n2), "SELECT 1, 2")
		c.Assert(err, IsNil)
		c.Assert(n1, Equals, 1)
		c.Assert(n2, Equals, 2)
	}

	c.Assert(tx.Rollback(), IsNil)
}

func (t *DBTest) TestIntegrityError(c *C) {
	_, err := t.db.Exec("DO $$BEGIN RAISE unique_violation USING MESSAGE='foo'; END$$;")
	c.Assert(err, FitsTypeOf, &pg.IntegrityError{})
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
	case <-time.After(4 * time.Second):
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

func (t *DBTest) TestCopyFrom(c *C) {
	data := "hello\t5\nworld\t5\nfoo\t3\nbar\t3\n"

	_, err := t.db.Exec("CREATE TEMP TABLE test(word text, len int)")
	c.Assert(err, IsNil)

	r := strings.NewReader(data)
	res, err := t.db.CopyFrom(r, "COPY test FROM STDIN")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 4)

	buf := &bytes.Buffer{}
	res, err = t.db.CopyTo(buf, "COPY test TO STDOUT")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 4)
	c.Assert(buf.String(), Equals, data)
}

func (t *DBTest) TestCopyTo(c *C) {
	_, err := t.db.Exec("CREATE TEMP TABLE test(n int)")
	c.Assert(err, IsNil)

	_, err = t.db.Exec("INSERT INTO test SELECT generate_series(1, 1000000)")
	c.Assert(err, IsNil)

	buf := &bytes.Buffer{}
	res, err := t.db.CopyTo(buf, "COPY test TO STDOUT")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 1000000)

	_, err = t.db.Exec("CREATE TEMP TABLE test2(n int)")
	c.Assert(err, IsNil)

	res, err = t.db.CopyFrom(buf, "COPY test2 FROM STDIN")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 1000000)
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
	dst := &structLoader{}
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

func (t *DBTest) BenchmarkQueryRowWithoutParams(c *C) {
	dst := &structLoader{}
	for i := 0; i < c.N; i++ {
		_, err := t.db.QueryOne(dst, "SELECT 1::bigint AS num")
		if err != nil {
			panic(err)
		}
		if dst.Num != 1 {
			panic("dst.Num != 1")
		}
	}
}

func (t *DBTest) BenchmarkQueryRowWithoutParamsStdlibPq(c *C) {
	var n int64
	for i := 0; i < c.N; i++ {
		r := t.pqdb.QueryRow("SELECT 1::bigint AS num")
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
		_, err := stmt.QueryOne(&structLoader{}, 1)
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
