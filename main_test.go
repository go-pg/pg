package pg_test

import (
	"bytes"
	"database/sql"
	"math"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	. "launchpad.net/gocheck"

	"gopkg.in/pg.v1"
	"gopkg.in/pg.v1/pgutil"
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

type discard struct{}

func (l *discard) New() interface{} {
	return l
}

func (l *discard) Load(colIdx int, colName string, b []byte) error {
	return nil
}

func (t *DBTest) TestQueryZeroRows(c *C) {
	res, err := t.db.Query(&discard{}, "SELECT 1 WHERE 1 != 1")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 0)
}

func (t *DBTest) TestQueryOneErrNoRows(c *C) {
	dst, err := t.db.QueryOne(&discard{}, "SELECT 1 WHERE 1 != 1")
	c.Assert(dst, IsNil)
	c.Assert(err, Equals, pg.ErrNoRows)
}

func (t *DBTest) TestQueryOneErrMultiRows(c *C) {
	dst, err := t.db.QueryOne(&discard{}, "SELECT generate_series(0, 10)")
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

func (t *DBTest) TestLoadInto(c *C) {
	var dst int
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT 1")
	c.Assert(err, IsNil)
	c.Assert(dst, Equals, 1)
}

func deref(v interface{}) interface{} {
	return reflect.ValueOf(v).Elem().Interface()
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

func (t *DBTest) TestIntegrityError(c *C) {
	_, err := t.db.Exec("DO $$BEGIN RAISE unique_violation USING MESSAGE='foo'; END$$;")
	c.Assert(err, FitsTypeOf, &pg.IntegrityError{})
}

func (t *DBTest) TestTypes(c *C) {
	var (
		b bool

		s  string
		bs []byte

		i    int
		i8   int8
		i16  int16
		i32  int32
		i64  int64
		ui   uint
		ui8  uint8
		ui16 uint16
		ui32 uint32
		ui64 uint64

		f32 float32
		f64 float64

		ss []string
		is []int

		sm map[string]string
	)
	table := []struct {
		src, dst interface{}
		typ      string
	}{
		{true, &b, "bool"},
		{false, &b, "bool"},

		{"hello world", &s, "text"},
		{[]byte("hello world\000"), &bs, "bytea"},

		{int(math.MaxInt32), &i, "int"},
		{int(math.MinInt32), &i, "int"},
		{int8(math.MaxInt8), &i8, "smallint"},
		{int8(math.MinInt8), &i8, "smallint"},
		{int16(math.MaxInt16), &i16, "smallint"},
		{int16(math.MinInt16), &i16, "smallint"},
		{int32(math.MaxInt32), &i32, "int"},
		{int32(math.MinInt32), &i32, "int"},
		{int64(math.MaxInt64), &i64, "bigint"},
		{int64(math.MinInt64), &i64, "bigint"},
		{uint(math.MaxUint32), &ui, "bigint"},
		{uint8(math.MaxUint8), &ui8, "smallint"},
		{uint16(math.MaxUint16), &ui16, "int"},
		{uint32(math.MaxUint32), &ui32, "bigint"},
		{uint64(math.MaxUint32), &ui64, "bigint"}, // uint64 is not supported

		{float32(math.MaxFloat32), &f32, "decimal"},
		{float32(math.SmallestNonzeroFloat32), &f32, "decimal"},
		{float64(math.MaxFloat64), &f64, "decimal"},
		{float64(math.SmallestNonzeroFloat64), &f64, "decimal"},

		{[]string{}, &ss, "text[]"},
		{[]string{"foo\n", "bar {}", "'\\\""}, &ss, "text[]"},
		{[]int{}, &is, "int[]"},
		{[]int{1, 2, 3}, &is, "int[]"},

		{map[string]string{"foo\n =>": "bar\n =>", "'\\\"": "'\\\""}, &sm, "hstore"},
	}

	t.db.Exec("CREATE EXTENSION hstore")
	defer t.db.Exec("DROP EXTENSION hstore")

	for _, row := range table {
		_, err := t.db.QueryOne(pg.LoadInto(row.dst), "SELECT ?", row.src)
		c.Assert(err, IsNil)
		c.Assert(deref(row.dst), DeepEquals, row.src)
	}

	for _, row := range table {
		if row.typ == "" {
			continue
		}

		stmt, err := t.db.Prepare("SELECT $1::" + row.typ)
		c.Assert(err, IsNil)

		_, err = stmt.QueryOne(pg.LoadInto(row.dst), row.src)
		c.Assert(err, IsNil)
		c.Assert(deref(row.dst), DeepEquals, row.src)

		c.Assert(stmt.Close(), IsNil)
	}
}

func (t *DBTest) TestTypeTime(c *C) {
	table := []struct {
		src time.Time
		dst time.Time
		typ string
	}{
		{time.Now(), time.Time{}, "timestamp with time zone"},
		{time.Now().UTC(), time.Time{}, "timestamp with time zone"},
		{time.Now(), time.Time{}, "timestamp"},
		{time.Now().UTC(), time.Time{}, "timestamp"},
	}

	for _, row := range table {
		_, err := t.db.QueryOne(pg.LoadInto(&row.dst), "SELECT ?", row.src)
		c.Assert(err, IsNil)
		c.Assert(row.dst.Unix(), DeepEquals, row.src.Unix())
	}

	for _, row := range table {
		if row.typ == "" {
			continue
		}

		stmt, err := t.db.Prepare("SELECT $1::" + row.typ)
		c.Assert(err, IsNil)

		_, err = stmt.QueryOne(pg.LoadInto(&row.dst), row.src)
		c.Assert(err, IsNil)
		c.Assert(row.dst.Unix(), DeepEquals, row.src.Unix())

		c.Assert(stmt.Close(), IsNil)
	}

	for _, row := range table {
		_, err := t.db.Exec("CREATE TEMP TABLE test_time (time ?)", pg.Q(row.typ))
		c.Assert(err, IsNil)

		_, err = t.db.Exec("INSERT INTO test_time VALUES (?)", row.src)
		c.Assert(err, IsNil)

		_, err = t.db.QueryOne(pg.LoadInto(&row.dst), "SELECT time FROM test_time")
		c.Assert(err, IsNil)
		c.Assert(row.dst.Unix(), Equals, row.src.Unix())
		if row.typ == "timestamp" {
			c.Assert(row.dst.Location(), Equals, time.UTC)
		}

		_, err = t.db.Exec("DROP TABLE test_time")
		c.Assert(err, IsNil)
	}
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
	res, err = t.db.CopyTo(&NopWriteCloser{buf}, "COPY test TO STDOUT")
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
	res, err := t.db.CopyTo(&NopWriteCloser{buf}, "COPY test TO STDOUT")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 1000000)

	_, err = t.db.Exec("CREATE TEMP TABLE test2(n int)")
	c.Assert(err, IsNil)

	res, err = t.db.CopyFrom(buf, "COPY test2 FROM STDIN")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 1000000)
}

func (t *DBTest) TestFormattingColumnNames(c *C) {
	c.Assert(pgutil.Underscore("Megacolumn"), Equals, "megacolumn")
	c.Assert(pgutil.Underscore("MegaColumn"), Equals, "mega_column")
	c.Assert(pgutil.Underscore("MegaColumn_Id"), Equals, "mega_column__id")
	c.Assert(pgutil.Underscore("MegaColumn_id"), Equals, "mega_column_id")
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
