package pg_test

import (
	"errors"

	. "gopkg.in/check.v1"

	"gopkg.in/pg.v5"
	"gopkg.in/pg.v5/orm"
)

type LoaderTest struct {
	db *pg.DB
}

var _ = Suite(&LoaderTest{})

func (t *LoaderTest) SetUpTest(c *C) {
	t.db = pg.Connect(pgOptions())
}

func (t *LoaderTest) TearDownTest(c *C) {
	c.Assert(t.db.Close(), IsNil)
}

type numLoader struct {
	Num int
}

type embeddedLoader struct {
	*numLoader
	Num2 int
}

type multipleLoader struct {
	One struct {
		Num int
	}
	Num int
}

func (t *LoaderTest) TestQuery(c *C) {
	var dst numLoader
	_, err := t.db.Query(&dst, "SELECT 1 AS num")
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 1)
}

func (t *LoaderTest) TestQueryNull(c *C) {
	var dst numLoader
	_, err := t.db.Query(&dst, "SELECT NULL AS num")
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 0)
}

func (t *LoaderTest) TestQueryEmbeddedStruct(c *C) {
	src := &embeddedLoader{
		numLoader: &numLoader{
			Num: 1,
		},
		Num2: 2,
	}
	dst := &embeddedLoader{
		numLoader: &numLoader{},
	}
	_, err := t.db.QueryOne(dst, "SELECT ?num AS num, ?num2 as num2", src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *LoaderTest) TestQueryNestedStructs(c *C) {
	src := &multipleLoader{}
	src.One.Num = 1
	src.Num = 2
	dst := &multipleLoader{}
	_, err := t.db.QueryOne(dst, `SELECT ?one__num AS one__num, ?num as num`, src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *LoaderTest) TestQueryStmt(c *C) {
	stmt, err := t.db.Prepare("SELECT 1 AS num")
	c.Assert(err, IsNil)
	defer stmt.Close()

	dst := &numLoader{}
	_, err = stmt.Query(dst)
	c.Assert(err, IsNil)
	c.Assert(dst.Num, Equals, 1)
}

func (t *LoaderTest) TestQueryInts(c *C) {
	var ids pg.Ints
	_, err := t.db.Query(&ids, "SELECT s.num AS num FROM generate_series(0, 10) AS s(num)")
	c.Assert(err, IsNil)
	c.Assert(ids, DeepEquals, pg.Ints{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
}

func (t *LoaderTest) TestQueryInts2(c *C) {
	var ints pg.Ints
	_, err := t.db.Query(&ints, "SELECT * FROM generate_series(1, 1000000)")
	c.Assert(err, IsNil)
	c.Assert(ints, HasLen, 1000000)
}

func (t *LoaderTest) TestQueryStrings(c *C) {
	var strings pg.Strings
	_, err := t.db.Query(&strings, "SELECT 'hello'")
	c.Assert(err, IsNil)
	c.Assert(strings, DeepEquals, pg.Strings{"hello"})
}

type errLoader string

var _ orm.Model = errLoader("")

func (m errLoader) NewModel() orm.ColumnScanner {
	return m
}

func (errLoader) AddModel(_ orm.ColumnScanner) error {
	return nil
}

func (errLoader) AfterQuery(_ orm.DB) error {
	return nil
}

func (errLoader) AfterSelect(_ orm.DB) error {
	return nil
}

func (errLoader) BeforeInsert(_ orm.DB) error {
	return nil
}

func (errLoader) AfterInsert(_ orm.DB) error {
	return nil
}

func (m errLoader) ScanColumn(int, string, []byte) error {
	return errors.New(string(m))
}

func (t *LoaderTest) TestLoaderError(c *C) {
	tx, err := t.db.Begin()
	c.Assert(err, IsNil)
	defer tx.Rollback()

	loader := errLoader("my error")
	_, err = tx.QueryOne(loader, "SELECT 1, 2")
	c.Assert(err.Error(), Equals, "my error")

	// Verify that client is still functional.
	var n1, n2 int
	_, err = tx.QueryOne(pg.Scan(&n1, &n2), "SELECT 1, 2")
	c.Assert(err, IsNil)
	c.Assert(n1, Equals, 1)
	c.Assert(n2, Equals, 2)
}
