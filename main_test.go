package pg_test

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	. "launchpad.net/gocheck"

	"gopkg.in/pg.v2"
	"gopkg.in/pg.v2/pgutil"
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
	_, err := t.db.QueryOne(&discard{}, "SELECT 1 WHERE 1 != 1")
	c.Assert(err, Equals, pg.ErrNoRows)
}

func (t *DBTest) TestQueryOneErrMultiRows(c *C) {
	_, err := t.db.QueryOne(&discard{}, "SELECT generate_series(0, 1)")
	c.Assert(err, Equals, pg.ErrMultiRows)
}

func (t *DBTest) TestExecOne(c *C) {
	res, err := t.db.ExecOne("SELECT 1")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 1)
}

func (t *DBTest) TestExecOneErrNoRows(c *C) {
	_, err := t.db.ExecOne("SELECT 1 WHERE 1 != 1")
	c.Assert(err, Equals, pg.ErrNoRows)
}

func (t *DBTest) TestExecOneErrMultiRows(c *C) {
	_, err := t.db.ExecOne("SELECT generate_series(0, 1)")
	c.Assert(err, Equals, pg.ErrMultiRows)
}

func (t *DBTest) TestLoadInto(c *C) {
	var dst int
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT 1")
	c.Assert(err, IsNil)
	c.Assert(dst, Equals, 1)
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

func (t *DBTest) TestLargeWriteRead(c *C) {
	src := bytes.Repeat([]byte{0x1}, 1e6)
	var dst []byte
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?", src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *DBTest) TestIntegrityError(c *C) {
	_, err := t.db.Exec("DO $$BEGIN RAISE unique_violation USING MESSAGE='foo'; END$$;")
	c.Assert(err, FitsTypeOf, &pg.IntegrityError{})
}

func deref(v interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(v)).Interface()
}

func zero(v interface{}) interface{} {
	return reflect.Zero(reflect.ValueOf(v).Elem().Type()).Interface()
}

type customStrSlice []string

func (s customStrSlice) Value() (driver.Value, error) {
	return strings.Join(s, "\n"), nil
}

func (s *customStrSlice) Scan(value interface{}) error {
	*s = strings.Split(string(value.([]byte)), "\n")
	return nil
}

var (
	boolv   bool
	boolptr *bool

	stringv   string
	stringptr *string
	bytesv    []byte

	intv    int
	int8v   int8
	int16v  int16
	int32v  int32
	int64v  int64
	uintv   uint
	uint8v  uint8
	uint16v uint16
	uint32v uint32
	uint64v uint64

	f32v float32
	f64v float64

	strslice    []string
	strsliceptr *[]string
	intslice    []int

	strstrmap map[string]string

	nullBool    sql.NullBool
	nullString  sql.NullString
	nullInt64   sql.NullInt64
	nullFloat64 sql.NullFloat64

	customStrSliceV customStrSlice

	timev   time.Time
	timeptr *time.Time
)

type conversionTest struct {
	src, dst interface{}
	pgtype   string

	wantnil  bool
	wantzero bool
}

var conversionTests = []conversionTest{
	{src: false, dst: &boolv, pgtype: "bool"},
	{src: true, dst: &boolv, pgtype: "bool"},
	{src: nil, dst: &boolv, pgtype: "bool", wantzero: true},
	{src: nil, dst: boolptr, pgtype: "bool", wantnil: true},

	{src: "hello world", dst: &stringv, pgtype: "text"},
	{src: nil, dst: &stringv, pgtype: "text", wantzero: true},
	{src: nil, dst: stringptr, pgtype: "text", wantnil: true},

	{src: []byte("hello world\000"), dst: &bytesv, pgtype: "bytea"},
	{src: nil, dst: &bytesv, pgtype: "bytea", wantzero: true},

	{src: int(math.MaxInt32), dst: &intv, pgtype: "int"},
	{src: int(math.MinInt32), dst: &intv, pgtype: "int"},
	{src: int8(math.MaxInt8), dst: &int8v, pgtype: "smallint"},
	{src: int8(math.MinInt8), dst: &int8v, pgtype: "smallint"},
	{src: int16(math.MaxInt16), dst: &int16v, pgtype: "smallint"},
	{src: int16(math.MinInt16), dst: &int16v, pgtype: "smallint"},
	{src: int32(math.MaxInt32), dst: &int32v, pgtype: "int"},
	{src: int32(math.MinInt32), dst: &int32v, pgtype: "int"},
	{src: int64(math.MaxInt64), dst: &int64v, pgtype: "bigint"},
	{src: int64(math.MinInt64), dst: &int64v, pgtype: "bigint"},
	{src: uint(math.MaxUint32), dst: &uintv, pgtype: "bigint"},
	{src: uint8(math.MaxUint8), dst: &uint8v, pgtype: "smallint"},
	{src: uint16(math.MaxUint16), dst: &uint16v, pgtype: "int"},
	{src: uint32(math.MaxUint32), dst: &uint32v, pgtype: "bigint"},
	{src: uint64(math.MaxUint32), dst: &uint64v, pgtype: "bigint"}, // math.MaxUint64 is not supported

	{src: float32(math.MaxFloat32), dst: &f32v, pgtype: "decimal"},
	{src: float32(math.SmallestNonzeroFloat32), dst: &f32v, pgtype: "decimal"},
	{src: float64(math.MaxFloat64), dst: &f64v, pgtype: "decimal"},
	{src: float64(math.SmallestNonzeroFloat64), dst: &f64v, pgtype: "decimal"},

	{src: []string{}, dst: &strslice, pgtype: "text[]"},
	{src: []string{"foo\n", "bar {}", "'\\\""}, dst: &strslice, pgtype: "text[]"},
	{src: nil, dst: &strslice, pgtype: "text[]", wantzero: true},
	{src: nil, dst: strsliceptr, pgtype: "text[]", wantnil: true},

	{src: []int{}, dst: &intslice, pgtype: "int[]"},
	{src: []int{1, 2, 3}, dst: &intslice, pgtype: "int[]"},

	{
		src:    map[string]string{"foo\n =>": "bar\n =>", "'\\\"": "'\\\""},
		dst:    &strstrmap,
		pgtype: "hstore",
	},

	{src: &sql.NullBool{}, dst: &nullBool, pgtype: "bool"},
	{src: &sql.NullBool{Valid: true}, dst: &nullBool, pgtype: "bool"},
	{src: &sql.NullBool{Valid: true, Bool: true}, dst: &nullBool, pgtype: "bool"},

	{src: &sql.NullString{}, dst: &nullString, pgtype: "text"},
	{src: &sql.NullString{Valid: true}, dst: &nullString, pgtype: "text"},
	{src: &sql.NullString{Valid: true, String: "foo"}, dst: &nullString, pgtype: "text"},

	{src: &sql.NullInt64{}, dst: &nullInt64, pgtype: "bigint"},
	{src: &sql.NullInt64{Valid: true}, dst: &nullInt64, pgtype: "bigint"},
	{src: &sql.NullInt64{Valid: true, Int64: math.MaxInt64}, dst: &nullInt64, pgtype: "bigint"},

	{src: &sql.NullFloat64{}, dst: &nullFloat64, pgtype: "decimal"},
	{src: &sql.NullFloat64{Valid: true}, dst: &nullFloat64, pgtype: "decimal"},
	{src: &sql.NullFloat64{Valid: true, Float64: math.MaxFloat64}, dst: &nullFloat64, pgtype: "decimal"},

	{src: customStrSlice{"one", "two"}, dst: &customStrSliceV},

	{src: time.Now(), dst: &timev, pgtype: "timestamp"},
	{src: time.Now().UTC(), dst: &timev, pgtype: "timestamp"},
	{src: nil, dst: &timev, pgtype: "timestamp", wantzero: true},
	{src: nil, dst: timeptr, pgtype: "timestamp", wantnil: true},
	{src: time.Now(), dst: &timev, pgtype: "timestamptz"},
	{src: time.Now().UTC(), dst: &timev, pgtype: "timestamptz"},
	{src: nil, dst: &timev, pgtype: "timestamptz", wantzero: true},
	{src: nil, dst: timeptr, pgtype: "timestamptz", wantnil: true},
}

func (t *conversionTest) Assert(c *C) {
	if t.wantzero {
		if reflect.ValueOf(t.dst).Elem().Kind() == reflect.Slice {
			c.Assert(t.dst, Not(IsNil))
			c.Assert(deref(t.dst), HasLen, 0)
		} else {
			c.Assert(deref(t.dst), Equals, zero(t.dst), t.Comment())
		}
		return
	}
	if t.wantnil {
		c.Assert(t.dst, IsNil)
		return
	}
	if dsttm, ok := t.dst.(*time.Time); ok {
		srctm := t.src.(time.Time)
		c.Assert(dsttm.Unix(), Equals, srctm.Unix())
	} else {
		c.Assert(deref(t.dst), DeepEquals, deref(t.src))
	}
}

func (t *conversionTest) Comment() CommentInterface {
	return Commentf("src: %#v, dst: %#v", t.src, t.dst)
}

func (t *DBTest) TestTypes(c *C) {
	t.db.Exec("CREATE EXTENSION hstore")
	defer t.db.Exec("DROP EXTENSION hstore")

	for _, row := range conversionTests {
		_, err := t.db.QueryOne(pg.LoadInto(row.dst), "SELECT ?", row.src)
		c.Assert(err, IsNil)
		row.Assert(c)
	}

	for _, row := range conversionTests {
		if row.pgtype == "" {
			continue
		}

		stmt, err := t.db.Prepare("SELECT $1::" + row.pgtype)
		c.Assert(err, IsNil)

		_, err = stmt.QueryOne(pg.LoadInto(row.dst), row.src)
		c.Assert(err, IsNil)
		c.Assert(stmt.Close(), IsNil)
		row.Assert(c)
	}

	for _, row := range conversionTests {
		dst := struct{ Dst interface{} }{Dst: row.dst}
		_, err := t.db.QueryOne(&dst, "SELECT ? AS dst", row.src)
		c.Assert(err, IsNil, row.Comment())
		row.Assert(c)
	}

	for _, row := range conversionTests {
		dst := struct{ Dst interface{} }{Dst: row.dst}
		_, err := t.db.QueryOne(&dst, "SELECT ? AS dst", row.src)
		c.Assert(err, IsNil, row.Comment())
		row.Assert(c)
	}

	for _, row := range conversionTests {
		if row.pgtype == "" {
			continue
		}

		stmt, err := t.db.Prepare(fmt.Sprintf("SELECT $1::%s AS dst", row.pgtype))
		c.Assert(err, IsNil)

		dst := struct{ Dst interface{} }{Dst: row.dst}
		_, err = stmt.QueryOne(&dst, row.src)
		c.Assert(err, IsNil)
		c.Assert(stmt.Close(), IsNil)
		row.Assert(c)
	}
}

func (t *DBTest) TestScannerValueOnStruct(c *C) {
	src := customStrSlice{"foo", "bar"}
	dst := struct{ Dst customStrSlice }{}
	_, err := t.db.QueryOne(&dst, "SELECT ? AS dst", src)
	c.Assert(err, IsNil)
	c.Assert(dst.Dst, DeepEquals, src)
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

func (t *DBTest) TestOverwritingNullValuesNonPointer(c *C) {
	rec := &struct {
		X int
	}{}
	_, err := t.db.QueryOne(rec, "SELECT 1138 AS x")
	c.Assert(err, IsNil)
	c.Assert(rec.X, Equals, 1138)

	_, err = t.db.QueryOne(rec, "SELECT NULL::int AS x")
	c.Assert(err, IsNil)
	c.Assert(rec.X, Equals, 0)
}

func (t *DBTest) TestOverwritingNullValuesPointer(c *C) {
	rec := &struct {
		X *int
	}{}
	_, err := t.db.QueryOne(rec, "SELECT 1138 AS x")
	c.Assert(err, IsNil)
	c.Assert(*rec.X, Equals, 1138)

	_, err = t.db.QueryOne(rec, "SELECT NULL::int AS x")
	c.Assert(err, IsNil)
	c.Assert(rec.X, IsNil)
}
