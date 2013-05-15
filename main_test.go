package pg_test

import (
	"database/sql"
	"math"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"
	. "launchpad.net/gocheck"

	"github.com/vmihailenco/pg"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&DBTest{})

type DBTest struct {
	db   *pg.DB
	godb *sql.DB
}

func (t *DBTest) SetUpTest(c *C) {
	connector := &pg.Connector{
		User:     "test",
		Database: "test",
		PoolSize: 2,
	}
	t.db = connector.Connect()

	db, err := sql.Open("postgres", "user=test dbname=test")
	c.Assert(err, IsNil)
	t.godb = db
}

func (t *DBTest) TearDownTest(c *C) {
	c.Assert(t.db.Close(), IsNil)
}

type Ids []int64

func (ids *Ids) New() interface{} {
	return ids
}

func (ids *Ids) Load(i int, b []byte) error {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*ids = append(*ids, n)
	return nil
}

type Dst struct {
	Num int
}

func (d *Dst) New() interface{} {
	return d
}

type Example struct {
	Id     uint64
	Names  []string
	Values map[string]string
	Data   []byte
}

type ExampleFabric struct{}

func (ExampleFabric) New() interface{} {
	return &Example{}
}

func (t *DBTest) TestQuery(c *C) {
	dst, err := t.db.Query(&Dst{}, "SELECT 1 AS num")
	c.Assert(err, IsNil)
	c.Assert(dst, HasLen, 1)
	c.Assert(dst[0].(*Dst).Num, Equals, 1)
}

func (t *DBTest) TestQueryOne(c *C) {
	dst, err := t.db.QueryOne(&Dst{}, "SELECT 1 AS num")
	c.Assert(err, IsNil)
	c.Assert(dst.(*Dst).Num, Equals, 1)
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

func (t *DBTest) TestTypeUint64(c *C) {
	var dst uint64
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?::bigint", uint64(math.MaxUint64))
	c.Assert(err, IsNil)
	c.Assert(dst, Equals, uint64(math.MaxUint64))
}

func (t *DBTest) TestTypeBytes(c *C) {
	var dst []byte
	_, err := t.db.QueryOne(pg.LoadInto(&dst), "SELECT ?::bytea", []byte("hello world"))
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, []byte("hello world"))
}

func (t *DBTest) TestTypeStringArray(c *C) {
	var dst []string
	_, err := t.db.QueryOne(
		pg.LoadInto(&dst),
		"SELECT ?::text[]",
		[]string{"foo \n", "bar", "hello {}", "'\\\""},
	)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, []string{"foo \n", "bar", "hello {}", "'\\\""})
}

func (t *DBTest) TestTypeIntArray(c *C) {
	var dst []int
	_, err := t.db.QueryOne(
		pg.LoadInto(&dst),
		"SELECT ?::int[]",
		[]int{1, 2, 3},
	)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, []int{1, 2, 3})
}

func (t *DBTest) TestTypeHstore(c *C) {
	dst := make(map[string]string)
	_, err := t.db.QueryOne(
		pg.LoadInto(&dst),
		"SELECT ?",
		map[string]string{"foo =>": "bar =>", "hello": "world", "'\\\"": "'\\\""},
	)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, map[string]string{"foo =>": "bar =>", "hello": "world", "'\\\"": "'\\\""})
}

func (t *DBTest) TestQueryIds(c *C) {
	var ids Ids
	_, err := t.db.Query(&ids, "SELECT s.num AS num FROM generate_series(0, 10) AS s(num)")
	c.Assert(err, IsNil)
	c.Assert(ids, DeepEquals, Ids{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
}

func (t *DBTest) TestExec(c *C) {
	res, err := t.db.Exec("CREATE TEMP TABLE test(id serial PRIMARY KEY)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, int64(0))

	res, err = t.db.Exec("INSERT INTO test VALUES (1)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, int64(1))
}

func (t *DBTest) TestStatementExec(c *C) {
	res, err := t.db.Exec("CREATE TEMP TABLE test(id serial PRIMARY KEY)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, int64(0))

	stmt, err := t.db.Prepare("INSERT INTO test VALUES($1)")
	c.Assert(err, IsNil)
	defer stmt.Close()

	res, err = stmt.Exec(1)
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, int64(1))
}

func (t *DBTest) TestStatementQuery(c *C) {
	stmt, err := t.db.Prepare("SELECT 1 AS num")
	c.Assert(err, IsNil)
	defer stmt.Close()

	dst, err := stmt.Query(&Dst{})
	c.Assert(err, IsNil)
	c.Assert(dst, HasLen, 1)
	c.Assert(dst[0].(*Dst).Num, Equals, 1)
}

func (t *DBTest) TestListenNotify(c *C) {
	listener, err := t.db.NewListener()
	c.Assert(err, IsNil)
	defer listener.Close()

	c.Assert(listener.Listen("test_channel"), IsNil)

	_, err = t.db.Exec("NOTIFY test_channel")
	c.Assert(err, IsNil)

	select {
	case notif := <-listener.Chan:
		c.Assert(notif.Err, IsNil)
		c.Assert(notif.Channel, Equals, "test_channel")
	case <-time.After(1 * time.Second):
		c.Fail()
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

func (t *DBTest) BenchmarkGoQueryRow(c *C) {
	var n int64
	for i := 0; i < c.N; i++ {
		r := t.godb.QueryRow("SELECT $1::bigint AS num", 1)
		if err := r.Scan(&n); err != nil {
			panic(err)
		}
		if n != 1 {
			panic("n != 1")
		}
	}
}

func (t *DBTest) BenchmarkExec(c *C) {
	_, err := t.db.Exec("CREATE TEMP TABLE test(id bigint)")
	if err != nil {
		panic(err)
	}

	for i := 0; i < c.N; i++ {
		_, err := t.db.Exec("INSERT INTO test VALUES(?)", 1)
		if err != nil {
			panic(err)
		}
	}
}

func (t *DBTest) BenchmarkStatementExec(c *C) {
	_, err := t.db.Exec("CREATE TEMP TABLE test(id bigint)")
	if err != nil {
		panic(err)
	}

	stmt, err := t.db.Prepare("INSERT INTO test VALUES($1)")
	c.Assert(err, IsNil)
	defer stmt.Close()

	for i := 0; i < c.N; i++ {
		_, err = stmt.Exec(1)
		if err != nil {
			panic(err)
		}
	}
}

func (t *DBTest) BenchmarkStatementQuery(c *C) {
	stmt, err := t.db.Prepare("SELECT 1 AS num")
	c.Assert(err, IsNil)
	defer stmt.Close()

	for i := 0; i < c.N; i++ {
		_, err := stmt.Query(&Dst{})
		if err != nil {
			panic(err)
		}
	}
}
