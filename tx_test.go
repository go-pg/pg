package pg_test

import (
	"strings"

	. "gopkg.in/check.v1"

	"gopkg.in/pg.v3"
)

var _ = Suite(&TxTest{})

type TxTest struct {
	db *pg.DB
}

func (t *TxTest) SetUpTest(c *C) {
	t.db = pg.Connect(pgOptions())
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

func (t *TxTest) TestCopyFromInTransaction(c *C) {
	data := "hello\t5\nworld\t5\nfoo\t3\nbar\t3\n"

	_, err := t.db.Exec("DROP TABLE IF EXISTS test_copy_from")
	c.Assert(err, IsNil)

	_, err = t.db.Exec("CREATE TABLE test_copy_from(word text, len int)")
	c.Assert(err, IsNil)

	tx1, err := t.db.Begin()
	c.Assert(err, IsNil)
	tx2, err := t.db.Begin()
	c.Assert(err, IsNil)

	r := strings.NewReader(data)
	res, err := tx1.CopyFrom(r, "COPY test_copy_from FROM STDIN")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 4)

	var count int
	_, err = tx1.QueryOne(pg.LoadInto(&count), "SELECT COUNT(*) FROM test_copy_from")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 4)

	_, err = tx2.QueryOne(pg.LoadInto(&count), "SELECT COUNT(*) FROM test_copy_from")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0)

	c.Assert(tx1.Commit(), IsNil)

	_, err = tx2.QueryOne(pg.LoadInto(&count), "SELECT COUNT(*) FROM test_copy_from")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 4) // assuming READ COMMITTED

	c.Assert(tx2.Rollback(), IsNil)

	_, err = t.db.Exec("DROP TABLE IF EXISTS test_copy_from")
	c.Assert(err, IsNil)
}

func (t *TxTest) TestCopyFromInTransactionWithErrors(c *C) {
	// too many fields on second line
	data := "hello\t5\nworld\t5\t6\t8\t9\nfoo\t3\nbar\t3\n"

	_, err := t.db.Exec("DROP TABLE IF EXISTS test_copy_from")
	c.Assert(err, IsNil)

	_, err = t.db.Exec("CREATE TABLE test_copy_from(word text, len int)")
	c.Assert(err, IsNil)
	_, err = t.db.Exec("INSERT INTO test_copy_from VALUES ('xxx', 3)")
	c.Assert(err, IsNil)

	tx1, err := t.db.Begin()
	c.Assert(err, IsNil)
	tx2, err := t.db.Begin()
	c.Assert(err, IsNil)

	_, err = tx1.Exec("INSERT INTO test_copy_from VALUES ('yyy', 3)")
	c.Assert(err, IsNil)

	r := strings.NewReader(data)
	_, err = tx1.CopyFrom(r, "COPY test_copy_from FROM STDIN")
	c.Assert(err, Not(IsNil))

	var count int
	_, err = tx1.QueryOne(pg.LoadInto(&count), "SELECT COUNT(*) FROM test_copy_from")
	c.Assert(err, Not(IsNil)) // transaction has errors, cannot proceed

	_, err = tx2.QueryOne(pg.LoadInto(&count), "SELECT COUNT(*) FROM test_copy_from")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)

	c.Assert(tx1.Commit(), IsNil) // actually ROLLBACK happens here

	_, err = tx2.QueryOne(pg.LoadInto(&count), "SELECT COUNT(*) FROM test_copy_from")
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1) // other transaction was rolled back so it's not 2 and not 6

	c.Assert(tx2.Rollback(), IsNil)

	_, err = t.db.Exec("DROP TABLE IF EXISTS test_copy_from")
	c.Assert(err, IsNil)
}
