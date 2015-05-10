package pg_test

import (
	. "gopkg.in/check.v1"

	"gopkg.in/pg.v3"
)

var _ = Suite(&TxTest{})

type TxTest struct {
	db *pg.DB
}

func (t *TxTest) SetUpTest(c *C) {
	t.db = pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "test",
		PoolSize: 10,
	})
}

func (t *TxTest) TearDownTest(c *C) {
	c.Assert(t.db.Close(), IsNil)
}

func (t *TxTest) TestMultiPrepare(c *C) {
	tx, err := t.db.Begin()
	c.Assert(err, IsNil)

	stmt1, err := tx.Prepare(`SELECT 'test_multi_prepare_tx1'`)
	c.Assert(err, IsNil)

	stmt2, err := tx.Prepare(`SELECT 'test_multi_prepare_tx2'`)
	c.Assert(err, IsNil)

	var s1 string
	_, err = stmt1.QueryOne(pg.LoadInto(&s1))
	c.Assert(err, IsNil)
	c.Assert(s1, Equals, "test_multi_prepare_tx1")

	var s2 string
	_, err = stmt2.QueryOne(pg.LoadInto(&s2))
	c.Assert(err, IsNil)
	c.Assert(s2, Equals, "test_multi_prepare_tx2")

	c.Assert(tx.Rollback(), IsNil)
}
