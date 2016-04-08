package pg_test

import (
	"bytes"
	"database/sql/driver"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	. "github.com/onsi/ginkgo"
	. "gopkg.in/check.v1"

	"gopkg.in/pg.v4"
)

func TestUnixSocket(t *testing.T) {
	opt := pgOptions()
	opt.Network = "unix"
	opt.Addr = "/var/run/postgresql/.s.PGSQL.5432"
	db := pg.Connect(opt)
	defer db.Close()

	_, err := db.Exec("SELECT 'test_unix_socket'")
	if err != nil {
		t.Fatal(err)
	}
}

func TestGocheck(t *testing.T) { TestingT(t) }

var _ = Suite(&DBTest{})

type DBTest struct {
	db *pg.DB
}

func (t *DBTest) SetUpTest(c *C) {
	t.db = pg.Connect(pgOptions())
}

func (t *DBTest) TearDownTest(c *C) {
	c.Assert(t.db.Close(), IsNil)
}

func (t *DBTest) TestQueryZeroRows(c *C) {
	res, err := t.db.Query(pg.Discard, "SELECT 1 WHERE 1 != 1")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 0)
}

func (t *DBTest) TestQueryOneErrNoRows(c *C) {
	_, err := t.db.QueryOne(pg.Discard, "SELECT 1 WHERE 1 != 1")
	c.Assert(err, Equals, pg.ErrNoRows)
}

func (t *DBTest) TestQueryOneErrMultiRows(c *C) {
	_, err := t.db.QueryOne(pg.Discard, "SELECT generate_series(0, 1)")
	c.Assert(err, Equals, pg.ErrMultiRows)
}

func (t *DBTest) TestExecOne(c *C) {
	res, err := t.db.ExecOne("SELECT 'test_exec_one'")
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

func (t *DBTest) TestScan(c *C) {
	var dst int
	_, err := t.db.QueryOne(pg.Scan(&dst), "SELECT 1")
	c.Assert(err, IsNil)
	c.Assert(dst, Equals, 1)
}

func (t *DBTest) TestExec(c *C) {
	res, err := t.db.Exec("CREATE TEMP TABLE test(id serial PRIMARY KEY)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, -1)

	res, err = t.db.Exec("INSERT INTO test VALUES (1)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, 1)
}

func (t *DBTest) TestStatementExec(c *C) {
	res, err := t.db.Exec("CREATE TEMP TABLE test(id serial PRIMARY KEY)")
	c.Assert(err, IsNil)
	c.Assert(res.Affected(), Equals, -1)

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
	_, err := t.db.QueryOne(pg.Scan(&dst), "SELECT ?", src)
	c.Assert(err, IsNil)
	c.Assert(dst, DeepEquals, src)
}

func (t *DBTest) TestIntegrityError(c *C) {
	_, err := t.db.Exec("DO $$BEGIN RAISE unique_violation USING MESSAGE='foo'; END$$;")
	c.Assert(err.(pg.Error).IntegrityViolation(), Equals, true)
}

type customStrSlice []string

func (s customStrSlice) Value() (driver.Value, error) {
	return strings.Join(s, "\n"), nil
}

func (s *customStrSlice) Scan(v interface{}) error {
	if v == nil {
		*s = nil
		return nil
	}

	b := v.([]byte)

	if len(b) == 0 {
		*s = []string{}
		return nil
	}

	*s = strings.Split(string(b), "\n")
	return nil
}

func (t *DBTest) TestScannerValueOnStruct(c *C) {
	src := customStrSlice{"foo", "bar"}
	dst := struct{ Dst customStrSlice }{}
	_, err := t.db.QueryOne(&dst, "SELECT ? AS dst", src)
	c.Assert(err, IsNil)
	c.Assert(dst.Dst, DeepEquals, src)
}

var timeTests = []struct {
	str    string
	wanted time.Time
}{
	{"2001-02-03", time.Date(2001, time.February, 3, 0, 0, 0, 0, time.UTC)},
	{"2001-02-03 04:05:06", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.Local)},
	{"2001-02-03 04:05:06.000001", time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.Local)},
	{"2001-02-03 04:05:06.00001", time.Date(2001, time.February, 3, 4, 5, 6, 10000, time.Local)},
	{"2001-02-03 04:05:06.0001", time.Date(2001, time.February, 3, 4, 5, 6, 100000, time.Local)},
	{"2001-02-03 04:05:06.001", time.Date(2001, time.February, 3, 4, 5, 6, 1000000, time.Local)},
	{"2001-02-03 04:05:06.01", time.Date(2001, time.February, 3, 4, 5, 6, 10000000, time.Local)},
	{"2001-02-03 04:05:06.1", time.Date(2001, time.February, 3, 4, 5, 6, 100000000, time.Local)},
	{"2001-02-03 04:05:06.12", time.Date(2001, time.February, 3, 4, 5, 6, 120000000, time.Local)},
	{"2001-02-03 04:05:06.123", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.Local)},
	{"2001-02-03 04:05:06.1234", time.Date(2001, time.February, 3, 4, 5, 6, 123400000, time.Local)},
	{"2001-02-03 04:05:06.12345", time.Date(2001, time.February, 3, 4, 5, 6, 123450000, time.Local)},
	{"2001-02-03 04:05:06.123456", time.Date(2001, time.February, 3, 4, 5, 6, 123456000, time.Local)},
	{"2001-02-03 04:05:06.123-07", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", -7*60*60))},
	{"2001-02-03 04:05:06-07", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -7*60*60))},
	{"2001-02-03 04:05:06-07:42", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+42*60)))},
	{"2001-02-03 04:05:06-07:30:09", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9)))},
	{"2001-02-03 04:05:06+07", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", 7*60*60))},
}

func (t *DBTest) TestTime(c *C) {
	for _, test := range timeTests {
		var tm time.Time
		_, err := t.db.QueryOne(pg.Scan(&tm), "SELECT ?", test.str)
		c.Assert(err, IsNil)
		c.Assert(tm.Unix(), Equals, test.wanted.Unix(), Commentf("str=%q", test.str))
	}
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

//------------------------------------------------------------------------------

// NopWriteCloser is a WriteCloser which does nothing in Close.
type NopWriteCloser struct {
	io.Writer
}

func (NopWriteCloser) Close() error {
	return nil
}

//------------------------------------------------------------------------------

type badConnError string

func (e badConnError) Error() string   { return string(e) }
func (e badConnError) Timeout() bool   { return false }
func (e badConnError) Temporary() bool { return false }

type badConn struct {
	net.TCPConn

	readDelay, writeDelay time.Duration
	readErr, writeErr     error
}

var _ net.Conn = &badConn{}

func (cn *badConn) Read([]byte) (int, error) {
	if cn.readDelay != 0 {
		time.Sleep(cn.readDelay)
	}
	if cn.readErr != nil {
		return 0, cn.readErr
	}
	return 0, badConnError("bad connection")
}

func (cn *badConn) Write([]byte) (int, error) {
	if cn.writeDelay != 0 {
		time.Sleep(cn.writeDelay)
	}
	if cn.writeErr != nil {
		return 0, cn.writeErr
	}
	return 0, badConnError("bad connection")
}

func perform(n int, cbs ...func(int)) {
	var wg sync.WaitGroup
	for _, cb := range cbs {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(cb func(int), i int) {
				defer GinkgoRecover()
				defer wg.Done()

				cb(i)
			}(cb, i)
		}
	}
	wg.Wait()
}
