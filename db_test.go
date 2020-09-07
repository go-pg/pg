package pg_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

func init() {
	orm.RegisterTable((*BookGenre)(nil))
}

func TestGinkgo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pg")
}

func pgOptions() *pg.Options {
	return &pg.Options{
		TLSConfig: getTLSConfig(),

		MaxRetries:      1,
		MinRetryBackoff: -1,

		DialTimeout:  30 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,

		PoolSize:           10,
		MaxConnAge:         10 * time.Second,
		PoolTimeout:        30 * time.Second,
		IdleTimeout:        10 * time.Second,
		IdleCheckFrequency: 100 * time.Millisecond,
	}
}

func getTLSConfig() *tls.Config {
	pgSSLMode := os.Getenv("PGSSLMODE")
	if pgSSLMode == "disable" {
		return nil
	}
	return &tls.Config{
		InsecureSkipVerify: true,
	}
}

var _testDB *pg.DB

func testDB() *pg.DB {
	if _testDB == nil {
		_testDB = pg.Connect(pgOptions())
	}
	return _testDB
}

func TestDBString(t *testing.T) {
	db := pg.Connect(pgOptions())
	defer db.Close()

	env := func(key, defValue string) string {
		envValue := os.Getenv(key)
		if envValue != "" {
			return envValue
		}
		return defValue
	}
	host := env("PGHOST", "localhost")
	port := env("PGPORT", "5432")

	wanted := fmt.Sprintf(`DB<Addr="%s:%s">`, host, port)
	if db.String() != wanted {
		t.Fatalf("got %q, wanted %q", db.String(), wanted)
	}

	db = db.WithParam("param1", "value1").WithParam("param2", 2)
	wanted = fmt.Sprintf(`DB<Addr="%s:%s" param1=value1 param2=2>`, host, port)
	if db.String() != wanted {
		t.Fatalf("got %q, wanted %q", db.String(), wanted)
	}
}

func TestOnConnect(t *testing.T) {
	opt := pgOptions()
	opt.OnConnect = func(ctx context.Context, db *pg.Conn) error {
		_, err := db.Exec("SET application_name = 'myapp'")
		return err
	}

	db := pg.Connect(opt)
	defer db.Close()

	var name string
	_, err := db.QueryOne(pg.Scan(&name), "SHOW application_name")
	if err != nil {
		t.Fatal(err)
	}
	if name != "myapp" {
		t.Fatalf(`got %q, wanted "myapp"`, name)
	}
}

func TestEmptyQuery(t *testing.T) {
	db := pg.Connect(pgOptions())
	defer db.Close()

	assert := func(err error) {
		if err == nil {
			t.Fatal("error expected")
		}
		if err.Error() != "pg: query is empty" {
			t.Fatal(err)
		}
	}

	_, err := db.Exec("")
	assert(err)

	_, err = db.Query(pg.Discard, "")
	assert(err)

	stmt, err := db.Prepare("")
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec()
	assert(err)
}

func TestAnonymousStructField(t *testing.T) {
	type MyInt struct{ int }

	type MyStruct struct {
		Ints []MyInt `pg:",array"`
	}

	db := testDB()

	var st MyStruct
	_, err := db.Query(&st, "SELECT ARRAY[1,2,3,4] AS ints")
	Expect(err).ToNot(BeNil())
	if !strings.Contains(err.Error(), "json: cannot unmarshal") {
		t.Fatal(err)
	}
}

func TestContextCanceled(t *testing.T) {
	db := testDB()

	c := context.Background()
	c, cancel := context.WithCancel(c)
	cancel()

	_, err := db.ExecContext(c, "SELECT 1")
	if err == nil {
		t.Fatalf("got nil, expected an error")
	}
	wanted := "context canceled"
	if err.Error() != wanted {
		t.Fatalf("got %q, wanted %q", err, wanted)
	}
}

func TestBigColumn(t *testing.T) {
	const colLen = 100000

	type Test struct {
		ID   int
		Text string
	}

	db := pg.Connect(pgOptions())
	defer db.Close()

	err := db.Model((*Test)(nil)).CreateTable(&orm.CreateTableOptions{
		Temp: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Model(&Test{
		Text: strings.Repeat("*", colLen),
	}).Insert()
	if err != nil {
		t.Fatal(err)
	}

	test := new(Test)
	err = db.Model(test).Select()
	if err != nil {
		t.Fatal(err)
	}
	if len(test.Text) != colLen {
		t.Fatalf("got %d, wanted %d", len(test.Text), colLen)
	}

	if _, err := db.Exec("SELECT * FROM tests"); err != nil {
		t.Fatal(err)
	}

	_, err = db.CopyTo(ioutil.Discard, "COPY (SELECT * FROM tests) TO STDOUT WITH CSV")
	if err != nil {
		t.Fatal(err)
	}
}

var _ = Describe("OnConnect", func() {
	It("does not panic on timeout", func() {
		opt := pgOptions()
		opt.OnConnect = func(ctx context.Context, conn *pg.Conn) error {
			_, err := conn.Exec("SELECT pg_sleep(10)")
			return err
		}

		db := pg.Connect(opt)
		defer db.Close()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(500 * time.Millisecond)
			cancel()
		}()

		_, err := db.ExecContext(ctx, "SELECT 1")
		Expect(err).To(MatchError("ERROR #57014 canceling statement due to user request"))
	})

	It("does not panic with RunInTransaction", func() {
		opt := pgOptions()
		opt.OnConnect = func(ctx context.Context, conn *pg.Conn) error {
			_, err := conn.Exec("SELECT 1")
			return err
		}

		db := pg.Connect(opt)
		defer db.Close()

		err := db.RunInTransaction(ctx, func(tx *pg.Tx) error {
			_, err := tx.Exec(`SELECT 1`)
			return err
		})
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("DB", func() {
	var db *pg.DB
	var tx *pg.Tx

	BeforeEach(func() {
		db = pg.Connect(pgOptions())

		var err error
		tx, err = db.Begin()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	Describe("uint64 in struct field", func() {
		It("is appended and scanned as int64", func() {
			type My struct {
				ID uint64 `pg:"type:bigint"`
			}

			err := db.Model((*My)(nil)).CreateTable(&orm.CreateTableOptions{
				Temp: true,
			})
			Expect(err).NotTo(HaveOccurred())

			my := &My{
				ID: math.MaxUint64,
			}
			_, err = db.Model(my).Insert()
			Expect(err).NotTo(HaveOccurred())

			my = &My{}
			err = db.Model(my).First()
			Expect(err).NotTo(HaveOccurred())
			Expect(my.ID).To(Equal(uint64(math.MaxUint64)))
		})
	})

	Describe("Query", func() {
		It("does not return an error when there are no results", func() {
			res, err := db.Query(pg.Discard, "SELECT 1 WHERE 1 = 2")
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RowsAffected()).To(Equal(0))

			res, err = tx.Query(pg.Discard, "SELECT 1 WHERE 1 = 2")
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RowsAffected()).To(Equal(0))
		})

		It("selects into embedded struct pointer", func() {
			type One struct {
				ID int
			}

			type Two struct {
				*One
			}

			two := new(Two)
			_, err := db.QueryOne(two, "SELECT 1 AS id")
			Expect(err).NotTo(HaveOccurred())
			Expect(two.One.ID).To(Equal(1))
		})
	})

	Describe("QueryOne", func() {
		It("returns pg.ErrNoRows when there are no results", func() {
			_, err := db.QueryOne(pg.Discard, "SELECT 1 WHERE 1 = 2")
			Expect(err).To(Equal(pg.ErrNoRows))

			_, err = tx.QueryOne(pg.Discard, "SELECT 1 WHERE 1 = 2")
			Expect(err).To(Equal(pg.ErrNoRows))
		})
	})

	Describe("Exec", func() {
		It("does not return an error when there are no results", func() {
			res, err := db.Exec("SELECT 1 WHERE 1 = 2")
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RowsAffected()).To(Equal(0))

			res, err = tx.Exec("SELECT 1 WHERE 1 = 2")
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RowsAffected()).To(Equal(0))
		})
	})

	Describe("ExecOne", func() {
		It("returns pg.ErrNoRows when there are no results", func() {
			_, err := db.ExecOne("SELECT 1 WHERE 1 = 2")
			Expect(err).To(Equal(pg.ErrNoRows))

			_, err = tx.ExecOne("SELECT 1 WHERE 1 = 2")
			Expect(err).To(Equal(pg.ErrNoRows))
		})
	})

	Describe("Prepare", func() {
		It("returns an error when query can't be prepared", func() {
			for i := 0; i < 3; i++ {
				_, err := db.Prepare("totally invalid sql")
				Expect(err).NotTo(BeNil())
				Expect(strings.Contains(err.Error(), "#42601")).To(BeTrue())
				Expect(strings.Contains(err.Error(), "syntax error")).To(BeTrue())

				_, err = db.Exec("SELECT 1")
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	Describe("Context", func() {
		It("cancels query when context is cancelled", func() {
			start := time.Now()

			c, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			_, err := db.ExecContext(c, "SELECT pg_sleep(5)")
			Expect(err).To(HaveOccurred())
			Expect(time.Since(start)).To(BeNumerically("~", time.Second, 100*time.Millisecond))
		})
	})
})

var _ = Describe("DB.Conn", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("does not acquire connection immediately", func() {
		conn := db.Conn()

		stats := db.PoolStats()
		Expect(stats.TotalConns).To(Equal(uint32(0)))

		err := conn.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("acquires connection when used and frees when closed", func() {
		conn := db.Conn()
		_, err := conn.Exec("SELECT 1")
		Expect(err).NotTo(HaveOccurred())

		stats := db.PoolStats()
		Expect(stats.TotalConns).To(Equal(uint32(1)))
		Expect(stats.IdleConns).To(Equal(uint32(0)))

		err = conn.Close()
		Expect(err).NotTo(HaveOccurred())

		stats = db.PoolStats()
		Expect(stats.TotalConns).To(Equal(uint32(1)))
		Expect(stats.IdleConns).To(Equal(uint32(1)))
	})

	It("supports Tx", func() {
		conn := db.Conn()

		tx, err := conn.Begin()
		Expect(err).NotTo(HaveOccurred())

		_, err = tx.Exec("SELECT 1")
		Expect(err).NotTo(HaveOccurred())

		_, err = conn.Exec("SELECT 1")
		Expect(err).NotTo(HaveOccurred())

		err = tx.Commit()
		Expect(err).NotTo(HaveOccurred())

		_, err = conn.Exec("SELECT 1")
		Expect(err).NotTo(HaveOccurred())

		err = conn.Close()
		Expect(err).NotTo(HaveOccurred())

		stats := db.PoolStats()
		Expect(stats.TotalConns).To(Equal(uint32(1)))
		Expect(stats.IdleConns).To(Equal(uint32(1)))
	})
})

var _ = Describe("Time", func() {
	tests := []struct {
		str    string
		wanted time.Time
	}{
		{"0001-01-01 00:00:00+00", time.Time{}},
		{"0000-01-01 00:00:00+00", time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC)},

		{"2001-02-03", time.Date(2001, time.February, 3, 0, 0, 0, 0, time.UTC)},
		{"2001-02-03 04:05:06", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.UTC)},
		{"2001-02-03 04:05:06.000001", time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.UTC)},
		{"2001-02-03 04:05:06.00001", time.Date(2001, time.February, 3, 4, 5, 6, 10000, time.UTC)},
		{"2001-02-03 04:05:06.0001", time.Date(2001, time.February, 3, 4, 5, 6, 100000, time.UTC)},
		{"2001-02-03 04:05:06.001", time.Date(2001, time.February, 3, 4, 5, 6, 1000000, time.UTC)},
		{"2001-02-03 04:05:06.01", time.Date(2001, time.February, 3, 4, 5, 6, 10000000, time.UTC)},
		{"2001-02-03 04:05:06.1", time.Date(2001, time.February, 3, 4, 5, 6, 100000000, time.UTC)},
		{"2001-02-03 04:05:06.12", time.Date(2001, time.February, 3, 4, 5, 6, 120000000, time.UTC)},
		{"2001-02-03 04:05:06.123", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.UTC)},
		{"2001-02-03 04:05:06.1234", time.Date(2001, time.February, 3, 4, 5, 6, 123400000, time.UTC)},
		{"2001-02-03 04:05:06.12345", time.Date(2001, time.February, 3, 4, 5, 6, 123450000, time.UTC)},
		{"2001-02-03 04:05:06.123456", time.Date(2001, time.February, 3, 4, 5, 6, 123456000, time.UTC)},
		{"2001-02-03 04:05:06.123-07", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", -7*60*60))},
		{"2001-02-03 04:05:06-07", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -7*60*60))},
		{"2001-02-03 04:05:06-07:42", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+42*60)))},
		{"2001-02-03 04:05:06-07:30:09", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -(7*60*60+30*60+9)))},
		{"2001-02-03 04:05:06+07", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", 7*60*60))},
	}

	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("is formatted correctly", func() {
		for i, test := range tests {
			var tm time.Time
			_, err := db.QueryOne(pg.Scan(&tm), "SELECT ?", test.wanted)
			Expect(err).NotTo(HaveOccurred())
			Expect(tm.Unix()).To(
				Equal(test.wanted.Unix()),
				"#%d str=%q wanted=%q", i, test.str, test.wanted,
			)
		}
	})

	It("is parsed correctly", func() {
		for i, test := range tests {
			var tm time.Time
			_, err := db.QueryOne(pg.Scan(&tm), "SELECT ?", test.str)
			Expect(err).NotTo(HaveOccurred())
			Expect(tm.Unix()).To(
				Equal(test.wanted.Unix()),
				"#%d str=%q wanted=%q", i, test.str, test.wanted,
			)
		}
	})
})

var _ = Describe("array model", func() {
	type value struct {
		Values []int16 `pg:",array"`
	}

	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("selects values", func() {
		model := new(value)
		_, err := db.QueryOne(model, "SELECT ? AS values", pg.Array([]int16{1, 2}))
		Expect(err).NotTo(HaveOccurred())
		Expect(model.Values).To(Equal([]int16{1, 2}))
	})

	It("selects empty values", func() {
		model := &value{
			Values: []int16{1, 2},
		}
		_, err := db.QueryOne(model, "SELECT ? AS values", pg.Array([]int16{}))
		Expect(err).NotTo(HaveOccurred())
		Expect(model.Values).To(BeEmpty())
	})

	It("selects null values", func() {
		model := &value{
			Values: []int16{1, 2},
		}
		_, err := db.QueryOne(model, "SELECT NULL AS values", pg.Array([]int16{}))
		Expect(err).NotTo(HaveOccurred())
		Expect(model.Values).To(BeEmpty())
	})
})

var _ = Describe("slice model", func() {
	type value struct {
		Id int
	}

	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("does not error when there are no rows", func() {
		ints := make([]int, 1)
		_, err := db.Query(&ints, "SELECT generate_series(1, 0)")
		Expect(err).NotTo(HaveOccurred())
		Expect(ints).To(BeEmpty())
	})

	It("does not error when there are no rows", func() {
		slice := make([]value, 1)
		_, err := db.Query(&slice, "SELECT generate_series(1, 0)")
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(BeEmpty())
	})

	It("does not error when there are no rows", func() {
		slice := make([]*value, 1)
		_, err := db.Query(&slice, "SELECT generate_series(1, 0)")
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(BeEmpty())
	})

	It("supports slice of structs", func() {
		var slice []value
		_, err := db.Query(&slice, `SELECT generate_series(1, 3) AS id`)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]value{{1}, {2}, {3}}))
	})

	It("supports slice of pointers", func() {
		var slice []*value
		_, err := db.Query(&slice, `SELECT generate_series(1, 3) AS id`)
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(Equal([]*value{{1}, {2}, {3}}))
	})

	It("supports Ints", func() {
		var ints pg.Ints
		_, err := db.Query(&ints, `SELECT generate_series(1, 3)`)
		Expect(err).NotTo(HaveOccurred())
		Expect(ints).To(Equal(pg.Ints{1, 2, 3}))
	})

	It("supports slice of ints", func() {
		var ints []int
		_, err := db.Query(&ints, `SELECT generate_series(1, 3)`)
		Expect(err).NotTo(HaveOccurred())
		Expect(ints).To(Equal([]int{1, 2, 3}))
	})

	It("supports slice of time.Time", func() {
		var times []time.Time
		_, err := db.Query(&times, `
			WITH data (time) AS (VALUES (clock_timestamp()), (clock_timestamp()))
			SELECT time FROM data
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(times).To(HaveLen(2))
	})

	It("resets slice", func() {
		ints := []int{1, 2, 3}
		_, err := db.Query(&ints, `SELECT 1`)
		Expect(err).NotTo(HaveOccurred())
		Expect(ints).To(Equal([]int{1}))
	})

	It("resets slice when there are no results", func() {
		ints := []int{1, 2, 3}
		_, err := db.Query(&ints, `SELECT 1 WHERE FALSE`)
		Expect(err).NotTo(HaveOccurred())
		Expect(ints).To(BeEmpty())
	})
})

var _ = Describe("read/write timeout", func() {
	var db *pg.DB

	BeforeEach(func() {
		opt := pgOptions()
		opt.ReadTimeout = time.Millisecond
		db = pg.Connect(opt)
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("slow query timeouts", func() {
		_, err := db.Exec(`SELECT pg_sleep(1)`)
		Expect(err.(net.Error).Timeout()).To(BeTrue())
	})

	Context("WithTimeout", func() {
		It("slow query passes", func() {
			_, err := db.WithTimeout(time.Minute).Exec(`SELECT pg_sleep(1)`)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("CopyFrom/CopyTo", func() {
	const n = 1000000
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())

		qs := []string{
			"CREATE TEMP TABLE copy_src(n int)",
			"CREATE TEMP TABLE copy_dst(n int)",
			fmt.Sprintf("INSERT INTO copy_src SELECT generate_series(1, %d)", n),
		}
		for _, q := range qs {
			_, err := db.Exec(q)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("copies data from a table and to a table", func() {
		var buf bytes.Buffer
		res, err := db.CopyTo(&buf, "COPY copy_src TO STDOUT")
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RowsAffected()).To(Equal(n))

		res, err = db.CopyFrom(&buf, "COPY copy_dst FROM STDIN")
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RowsAffected()).To(Equal(n))

		st := db.PoolStats()
		Expect(st.Hits).To(Equal(uint32(4)))
		Expect(st.Misses).To(Equal(uint32(1)))
		Expect(st.Timeouts).To(Equal(uint32(0)))
		Expect(st.TotalConns).To(Equal(uint32(1)))
		Expect(st.IdleConns).To(Equal(uint32(1)))

		var count int
		_, err = db.QueryOne(pg.Scan(&count), "SELECT count(*) FROM copy_dst")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(n))
	})

	It("copies corrupted data to a table", func() {
		buf := bytes.NewBufferString("corrupted,data\nrow,two\r\nrow three")
		res, err := db.CopyFrom(buf, "COPY copy_dst FROM STDIN WITH FORMAT csv")
		Expect(err).To(MatchError(`ERROR #42601 syntax error at or near "FORMAT"`))
		Expect(res).To(BeNil())

		st := db.Pool().Stats()
		Expect(st.Hits).To(Equal(uint32(3)))
		Expect(st.Misses).To(Equal(uint32(1)))
		Expect(st.Timeouts).To(Equal(uint32(0)))
		Expect(st.TotalConns).To(Equal(uint32(1)))
		Expect(st.IdleConns).To(Equal(uint32(1)))

		var count int
		_, err = db.QueryOne(pg.Scan(&count), "SELECT count(*) FROM copy_dst")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(0))
	})
})

var _ = Describe("CountEstimate", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("works", func() {
		count, err := db.Model().
			TableExpr("generate_series(1, 10)").
			CountEstimate(1000)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(10))
	})

	It("works when there are no results", func() {
		count, err := db.Model().
			TableExpr("generate_series(1, 0)").
			CountEstimate(1000)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(0))
	})

	It("works with GROUP", func() {
		count, err := db.Model().
			TableExpr("generate_series(1, 10)").
			Group("generate_series").
			CountEstimate(1000)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(10))
	})

	It("works with GROUP when there are no results", func() {
		count, err := db.Model().
			TableExpr("generate_series(1, 0)").
			Group("generate_series").
			CountEstimate(1000)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(0))
	})
})

var _ = Describe("DB nulls", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())

		_, err := db.Exec("CREATE TEMP TABLE tests (id int, value int)")
		Expect(err).To(BeNil())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	Describe("sql.NullInt64", func() {
		type Test struct {
			ID    int
			Value sql.NullInt64
		}

		It("inserts null value", func() {
			ins := &Test{
				ID: 1,
			}
			_, err := db.Model(ins).Insert(ins)
			Expect(err).NotTo(HaveOccurred())

			sel := &Test{
				ID: 1,
			}
			err = db.Model(sel).WherePK().Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(sel.Value.Valid).To(BeFalse())
		})

		It("inserts non-null value", func() {
			ins := &Test{
				ID: 1,
				Value: sql.NullInt64{
					Int64: 2,
					Valid: true,
				},
			}
			_, err := db.Model(ins).Insert()
			Expect(err).NotTo(HaveOccurred())

			sel := &Test{
				ID: 1,
			}
			err = db.Model(sel).WherePK().Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(sel.Value.Valid).To(BeTrue())
			Expect(sel.Value.Int64).To(Equal(int64(2)))
		})
	})

	Context("nil ptr", func() {
		type Test struct {
			ID    int
			Value *int
		}

		It("inserts null value", func() {
			ins := &Test{
				ID: 1,
			}
			_, err := db.Model(ins).Insert()
			Expect(err).NotTo(HaveOccurred())

			sel := &Test{
				ID: 1,
			}
			err = db.Model(sel).WherePK().Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(sel.Value).To(BeNil())
		})

		It("inserts non-null value", func() {
			value := 2
			ins := &Test{
				ID:    1,
				Value: &value,
			}
			_, err := db.Model(ins).Insert()
			Expect(err).NotTo(HaveOccurred())

			sel := &Test{
				ID: 1,
			}
			err = db.Model(sel).WherePK().Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(sel.Value).NotTo(BeNil())
			Expect(*sel.Value).To(Equal(2))
		})
	})
})

var _ = Describe("DB.Select", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("selects bytea", func() {
		qs := []string{
			`CREATE TEMP TABLE tests (col bytea)`,
			fmt.Sprintf(`INSERT INTO tests VALUES ('\x%x')`, []byte("bytes")),
		}
		for _, q := range qs {
			_, err := db.Exec(q)
			Expect(err).NotTo(HaveOccurred())
		}

		var col []byte
		err := db.Model().Table("tests").Column("col").Select(pg.Scan(&col))
		Expect(err).NotTo(HaveOccurred())
	})

	It("selects into embedded struct pointer", func() {
		type One struct {
			ID int
		}

		type Two struct {
			*One
		}

		err := db.Model((*Two)(nil)).CreateTable(&orm.CreateTableOptions{
			Temp: true,
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = db.Model(&Two{
			One: &One{
				ID: 1,
			},
		}).Insert()
		Expect(err).NotTo(HaveOccurred())

		two := new(Two)
		err = db.Model(two).Where("id = 1").Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(two.One.ID).To(Equal(1))
	})
})

var _ = Describe("DB.Insert", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("returns an error on nil", func() {
		_, err := db.Model(nil).Insert()
		Expect(err).To(MatchError("pg: Model(nil)"))
	})

	It("returns an error if value is not settable", func() {
		_, err := db.Model(1).Insert()
		Expect(err).To(MatchError("pg: Model(non-pointer int)"))
	})

	It("returns an error if value is not supported", func() {
		var v int
		_, err := db.Model(&v).Insert()
		Expect(err).To(MatchError("pg: Model(unsupported *int)"))
	})
})

var _ = Describe("DB.Update", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("returns an error on nil", func() {
		_, err := db.Model(nil).Update()
		Expect(err).To(MatchError("pg: Model(nil)"))
	})

	It("returns an error if there are no pks", func() {
		type Test struct{}
		var test Test
		_, err := db.Model(&test).WherePK().Update()
		Expect(err).To(MatchError(`pg: model=Test does not have primary keys`))
	})
})

var _ = Describe("DB.Delete", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("returns an error on nil", func() {
		_, err := db.Model(nil).Delete()
		Expect(err).To(MatchError("pg: Model(nil)"))
	})

	It("returns an error if there are no pks", func() {
		type Test struct{}
		var test Test
		_, err := db.Model(&test).WherePK().Delete()
		Expect(err).To(MatchError(`pg: model=Test does not have primary keys`))
	})

	It("returns an error if there are no where", func() {
		var test []struct {
			Id int
		}
		_, err := db.Model(&test).Delete()
		Expect(err).To(MatchError(`pg: Update and Delete queries require Where clause (try WherePK)`))
	})
})

var _ = Describe("errors", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("unknown column error", func() {
		type Test struct {
			Col1 int
		}

		var test Test
		_, err := db.QueryOne(&test, "SELECT 1 AS col1, 2 AS col2")
		Expect(err).To(MatchError("pg: can't find column=col2 in model=Test (prefix the column with underscore or use discard_unknown_columns)"))
		Expect(test.Col1).To(Equal(1))
	})

	It("Scan error", func() {
		var n1 int
		_, err := db.QueryOne(pg.Scan(&n1), "SELECT 1, 2")
		Expect(err).To(MatchError(`pg: no Scan var for column index=1 name="?column?"`))
		Expect(n1).To(Equal(1))
	})
})

type Genre struct {
	// tableName is an optional field that specifies custom table name and alias.
	// By default go-pg generates table name and alias from struct name.
	tableName struct{} `pg:"genres,alias:genre"` // default values are the same

	ID     int
	Name   string
	Rating int `pg:"-"` // - is used to ignore field

	Books []Book `pg:"many2many:book_genres"`

	ParentID  int
	Subgenres []Genre `pg:"rel:has-many,join_fk:parent_id"`
}

func (g Genre) String() string {
	return fmt.Sprintf("Genre<Id=%d Name=%q>", g.ID, g.Name)
}

type Image struct {
	ID   int
	Path string
}

type Author struct {
	ID    int
	Name  string  `pg:",unique"`
	Books []*Book `pg:"rel:has-many"`

	AvatarID int
	Avatar   Image `pg:"rel:has-one"`
}

func (a Author) String() string {
	return fmt.Sprintf("Author<ID=%d Name=%q>", a.ID, a.Name)
}

type BookGenre struct {
	tableName struct{} `pg:"alias:bg"` // custom table alias

	BookID  int    `pg:",pk"` // pk tag is used to mark field as primary key
	Book    *Book  `pg:"rel:has-one"`
	GenreID int    `pg:",pk"`
	Genre   *Genre `pg:"rel:has-one"`

	Genre_Rating int // belongs to and is copied to Genre model
}

type Book struct {
	ID        int
	Title     string
	AuthorID  int
	Author    Author `pg:"rel:has-one"`
	EditorID  int
	Editor    *Author   `pg:"rel:has-one"`
	CreatedAt time.Time `pg:"default:now()"`
	UpdatedAt time.Time

	Genres       []Genre       `pg:"many2many:book_genres"` // many to many relation
	Translations []Translation `pg:"rel:has-many"`
	Comments     []Comment     `pg:"rel:has-many,join_fk:trackable_,polymorphic"`
}

var _ orm.BeforeInsertHook = (*Book)(nil)

func (b Book) String() string {
	return fmt.Sprintf("Book<Id=%d Title=%q>", b.ID, b.Title)
}

func (b *Book) BeforeInsert(c context.Context) (context.Context, error) {
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now()
	}
	return c, nil
}

// BookWithCommentCount is like Book model, but has additional CommentCount
// field that is used to select data into it. The use of `pg:",inherit"` tag
// is essential here so it inherits internal model properties such as table name.
type BookWithCommentCount struct {
	Book `pg:",inherit"`

	CommentCount int
}

type Translation struct {
	tableName struct{} `pg:",alias:tr"` // custom table alias

	ID     int
	BookID int    `pg:"unique:book_id_lang"`
	Book   *Book  `pg:"rel:has-one"`
	Lang   string `pg:"unique:book_id_lang"`

	Comments []Comment `pg:"rel:has-many,join_fk:trackable_,polymorphic"`
}

type Comment struct {
	TrackableID   int    // Book.ID or Translation.ID
	TrackableType string // "Book" or "Translation"
	Text          string
}

func createTestSchema(db *pg.DB) error {
	models := []interface{}{
		(*Image)(nil),
		(*Author)(nil),
		(*Book)(nil),
		(*Genre)(nil),
		(*BookGenre)(nil),
		(*Translation)(nil),
		(*Comment)(nil),
	}
	for _, model := range models {
		err := db.Model(model).DropTable(&orm.DropTableOptions{
			IfExists: true,
			Cascade:  true,
		})
		if err != nil {
			return err
		}

		err = db.Model(model).CreateTable(nil)
		if err != nil {
			return err
		}
	}
	return nil
}

var _ = Describe("ORM", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())

		err := createTestSchema(db)
		Expect(err).NotTo(HaveOccurred())

		genres := []Genre{{
			ID:   1,
			Name: "genre 1",
		}, {
			ID:   2,
			Name: "genre 2",
		}, {
			ID:       3,
			Name:     "subgenre 1",
			ParentID: 1,
		}, {
			ID:       4,
			Name:     "subgenre 2",
			ParentID: 1,
		}}
		_, err = db.Model(&genres).Insert()
		Expect(err).NotTo(HaveOccurred())
		Expect(genres).To(HaveLen(4))

		images := []Image{{
			ID:   1,
			Path: "/path/to/1.jpg",
		}, {
			ID:   2,
			Path: "/path/to/2.jpg",
		}, {
			ID:   3,
			Path: "/path/to/3.jpg",
		}}
		_, err = db.Model(&images).Insert()
		Expect(err).NotTo(HaveOccurred())
		Expect(images).To(HaveLen(3))

		authors := []Author{{
			ID:       10,
			Name:     "author 1",
			AvatarID: images[0].ID,
		}, {
			ID:       11,
			Name:     "author 2",
			AvatarID: images[1].ID,
		}, {
			ID:       12,
			Name:     "author 3",
			AvatarID: images[2].ID,
		}}
		_, err = db.Model(&authors).Insert()
		Expect(err).NotTo(HaveOccurred())
		Expect(authors).To(HaveLen(3))

		books := []Book{{
			ID:       100,
			Title:    "book 1",
			AuthorID: 10,
			EditorID: 11,
		}, {
			ID:       101,
			Title:    "book 2",
			AuthorID: 10,
			EditorID: 12,
		}, {
			ID:       102,
			Title:    "book 3",
			AuthorID: 11,
			EditorID: 11,
		}}
		_, err = db.Model(&books).Insert()
		Expect(err).NotTo(HaveOccurred())
		Expect(books).To(HaveLen(3))
		for _, book := range books {
			Expect(book.CreatedAt).To(BeTemporally("~", time.Now(), time.Second))
		}

		bookGenres := []BookGenre{{
			BookID:       100,
			GenreID:      1,
			Genre_Rating: 999,
		}, {
			BookID:       100,
			GenreID:      2,
			Genre_Rating: 9999,
		}, {
			BookID:       101,
			GenreID:      1,
			Genre_Rating: 99999,
		}}
		_, err = db.Model(&bookGenres).Insert()
		Expect(err).NotTo(HaveOccurred())
		Expect(bookGenres).To(HaveLen(3))

		translations := []Translation{{
			ID:     1000,
			BookID: 100,
			Lang:   "ru",
		}, {
			ID:     1001,
			BookID: 100,
			Lang:   "md",
		}, {
			ID:     1002,
			BookID: 101,
			Lang:   "ua",
		}}
		_, err = db.Model(&translations).Insert()
		Expect(err).NotTo(HaveOccurred())
		Expect(translations).To(HaveLen(3))

		comments := []Comment{{
			TrackableID:   100,
			TrackableType: "Book",
			Text:          "comment1",
		}, {
			TrackableID:   100,
			TrackableType: "Book",
			Text:          "comment2",
		}, {
			TrackableID:   1000,
			TrackableType: "Translation",
			Text:          "comment3",
		}}
		_, err = db.Model(&comments).Insert()
		Expect(err).NotTo(HaveOccurred())
		Expect(comments).To(HaveLen(3))
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	Describe("relation with no results", func() {
		It("does not panic", func() {
			tr := new(Translation)
			tr.ID = 123
			_, err := db.Model(tr).Insert()
			Expect(err).NotTo(HaveOccurred())

			err = db.Model(tr).
				Relation("Book.Genres").
				Relation("Book.Translations").
				Relation("Book.Comments").
				WherePK().
				Select()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("struct model", func() {
		It("Select returns pg.ErrNoRows", func() {
			book := new(Book)
			err := db.Model(book).
				Where("1 = 2").
				Select()
			Expect(err).To(Equal(pg.ErrNoRows))
		})

		It("Insert returns pg.ErrNoRows", func() {
			book := new(Book)
			err := db.Model(book).First()
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Model(book).
				OnConflict("DO NOTHING").
				Returning("*").
				Insert()
			Expect(err).To(Equal(pg.ErrNoRows))
		})

		It("Update returns pg.ErrNoRows", func() {
			book := new(Book)
			_, err := db.Model(book).
				Where("1 = 2").
				Returning("*").
				Update()
			Expect(err).To(Equal(pg.ErrNoRows))
		})

		It("Delete returns pg.ErrNoRows", func() {
			book := new(Book)
			_, err := db.Model(book).
				Where("1 = 2").
				Returning("*").
				Delete()
			Expect(err).To(Equal(pg.ErrNoRows))
		})

		It("fetches Book relations", func() {
			book := new(Book)
			err := db.Model(book).
				Column("book.id").
				Relation("Author").
				Relation("Author.Avatar").
				Relation("Editor").
				Relation("Editor.Avatar").
				Relation("Genres").
				Relation("Comments").
				Relation("Translations", func(q *orm.Query) (*orm.Query, error) {
					return q.Order("id"), nil
				}).
				Relation("Translations.Comments", func(q *orm.Query) (*orm.Query, error) {
					return q.Order("text"), nil
				}).
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(book).To(Equal(&Book{
				ID:    100,
				Title: "",
				Author: Author{
					ID:       10,
					Name:     "author 1",
					AvatarID: 1,
					Avatar: Image{
						ID:   1,
						Path: "/path/to/1.jpg",
					},
				},
				Editor: &Author{
					ID:       11,
					Name:     "author 2",
					AvatarID: 2,
					Avatar: Image{
						ID:   2,
						Path: "/path/to/2.jpg",
					},
				},
				CreatedAt: time.Time{},
				Genres: []Genre{
					{ID: 1, Name: "genre 1", Rating: 999},
					{ID: 2, Name: "genre 2", Rating: 9999},
				},
				Translations: []Translation{{
					ID:     1000,
					BookID: 100,
					Lang:   "ru",
					Comments: []Comment{
						{TrackableID: 1000, TrackableType: "Translation", Text: "comment3"},
					},
				}, {
					ID:       1001,
					BookID:   100,
					Lang:     "md",
					Comments: nil,
				}},
				Comments: []Comment{
					{TrackableID: 100, TrackableType: "Book", Text: "comment1"},
					{TrackableID: 100, TrackableType: "Book", Text: "comment2"},
				},
			}))
		})

		It("fetches Author relations", func() {
			var author Author
			err := db.Model(&author).
				Column("author.*").
				Relation("Books.id").Relation("Books.author_id").Relation("Books.editor_id").
				Relation("Books.Author").
				Relation("Books.Editor").
				Relation("Books.Translations").
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(author).To(Equal(Author{
				ID:       10,
				Name:     "author 1",
				AvatarID: 1,
				Books: []*Book{{
					ID:        100,
					Title:     "",
					AuthorID:  10,
					Author:    Author{ID: 10, Name: "author 1", AvatarID: 1},
					EditorID:  11,
					Editor:    &Author{ID: 11, Name: "author 2", AvatarID: 2},
					CreatedAt: time.Time{},
					Genres:    nil,
					Translations: []Translation{
						{ID: 1000, BookID: 100, Book: nil, Lang: "ru", Comments: nil},
						{ID: 1001, BookID: 100, Book: nil, Lang: "md", Comments: nil},
					},
				}, {
					ID:        101,
					Title:     "",
					AuthorID:  10,
					Author:    Author{ID: 10, Name: "author 1", AvatarID: 1},
					EditorID:  12,
					Editor:    &Author{ID: 12, Name: "author 3", AvatarID: 3},
					CreatedAt: time.Time{},
					Genres:    nil,
					Translations: []Translation{
						{ID: 1002, BookID: 101, Book: nil, Lang: "ua", Comments: nil},
					},
				}},
			}))
		})

		It("fetches Genre relations", func() {
			var genre Genre
			err := db.Model(&genre).
				Column("genre.*").
				Relation("Books.id").
				Relation("Books.Translations").
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(genre).To(Equal(Genre{
				ID:     1,
				Name:   "genre 1",
				Rating: 0,
				Books: []Book{{
					ID: 100,
					Translations: []Translation{
						{ID: 1000, BookID: 100, Book: nil, Lang: "ru", Comments: nil},
						{ID: 1001, BookID: 100, Book: nil, Lang: "md", Comments: nil},
					},
				}, {
					ID: 101,
					Translations: []Translation{
						{ID: 1002, BookID: 101, Book: nil, Lang: "ua", Comments: nil},
					},
				}},
				ParentID:  0,
				Subgenres: nil,
			}))
		})

		It("fetches Translation relation", func() {
			var translation Translation
			err := db.Model(&translation).
				Column("tr.*").
				Relation("Book.id").
				Relation("Book.Author").
				Relation("Book.Editor").
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(translation).To(Equal(Translation{
				ID:     1000,
				BookID: 100,
				Book: &Book{
					ID:     100,
					Author: Author{ID: 10, Name: "author 1", AvatarID: 1},
					Editor: &Author{ID: 11, Name: "author 2", AvatarID: 2},
				},
				Lang: "ru",
			}))
		})

		It("works when there are no results", func() {
			book := new(Book)
			err := db.Model(book).
				Column("book.*").
				Relation("Author").
				Relation("Genres").
				Relation("Comments").
				Where("1 = 2").
				Select()
			Expect(err).To(Equal(pg.ErrNoRows))
		})

		It("supports overriding", func() {
			book := new(BookWithCommentCount)
			err := db.Model(book).
				Column("book.id").
				Relation("Author").
				Relation("Genres").
				ColumnExpr(`(SELECT COUNT(*) FROM comments
					WHERE trackable_type = 'Book' AND
					trackable_id = book.id) AS comment_count`).
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(book).To(Equal(&BookWithCommentCount{
				Book: Book{
					ID:     100,
					Author: Author{ID: 10, Name: "author 1", AvatarID: 1},
					Genres: []Genre{
						{ID: 1, Name: "genre 1", Rating: 999},
						{ID: 2, Name: "genre 2", Rating: 9999},
					},
				},
				CommentCount: 2,
			}))
		})
	})

	Describe("slice model", func() {
		It("fetches Book relations", func() {
			var books []Book
			err := db.Model(&books).
				Column("book.id").
				Relation("Author").
				Relation("Author.Avatar").
				Relation("Editor").
				Relation("Editor.Avatar").
				Relation("Genres").
				Relation("Comments").
				Relation("Translations").
				Relation("Translations.Comments").
				OrderExpr("book.id ASC").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(Equal([]Book{{
				ID:       100,
				Title:    "",
				AuthorID: 0,
				Author: Author{
					ID:       10,
					Name:     "author 1",
					AvatarID: 1,
					Avatar: Image{
						ID:   1,
						Path: "/path/to/1.jpg",
					},
				},
				EditorID: 0,
				Editor: &Author{
					ID:       11,
					Name:     "author 2",
					AvatarID: 2,
					Avatar: Image{
						ID:   2,
						Path: "/path/to/2.jpg",
					},
				},
				Genres: []Genre{
					{ID: 1, Name: "genre 1", Rating: 999},
					{ID: 2, Name: "genre 2", Rating: 9999},
				},
				Translations: []Translation{{
					ID:     1000,
					BookID: 100,
					Lang:   "ru",
					Comments: []Comment{
						{TrackableID: 1000, TrackableType: "Translation", Text: "comment3"},
					},
				}, {
					ID:       1001,
					BookID:   100,
					Lang:     "md",
					Comments: nil,
				}},
				Comments: []Comment{
					{TrackableID: 100, TrackableType: "Book", Text: "comment1"},
					{TrackableID: 100, TrackableType: "Book", Text: "comment2"},
				},
			}, {
				ID:       101,
				Title:    "",
				AuthorID: 0,
				Author: Author{
					ID:       10,
					Name:     "author 1",
					AvatarID: 1,
					Avatar: Image{
						ID:   1,
						Path: "/path/to/1.jpg",
					},
				},
				EditorID: 0,
				Editor: &Author{
					ID:       12,
					Name:     "author 3",
					AvatarID: 3,
					Avatar: Image{
						ID:   3,
						Path: "/path/to/3.jpg",
					},
				},
				Genres: []Genre{
					{ID: 1, Name: "genre 1", Rating: 99999},
				},
				Translations: []Translation{
					{ID: 1002, BookID: 101, Lang: "ua"},
				},
			}, {
				ID:       102,
				Title:    "",
				AuthorID: 0,
				Author: Author{
					ID:       11,
					Name:     "author 2",
					AvatarID: 2,
					Avatar: Image{
						ID:   2,
						Path: "/path/to/2.jpg",
					},
				},
				EditorID: 0,
				Editor: &Author{
					ID:       11,
					Name:     "author 2",
					AvatarID: 2,
					Avatar: Image{
						ID:   2,
						Path: "/path/to/2.jpg",
					},
				},
			}}))
		})

		It("fetches Genre relations", func() {
			var genres []Genre
			err := db.Model(&genres).
				Column("genre.*").
				Relation("Subgenres").
				Relation("Books.id").
				Relation("Books.Translations").
				Where("genre.parent_id IS NULL").
				OrderExpr("genre.id").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(genres).To(Equal([]Genre{
				{
					ID:     1,
					Name:   "genre 1",
					Rating: 0,
					Books: []Book{{
						ID: 100,
						Translations: []Translation{
							{ID: 1000, BookID: 100, Book: nil, Lang: "ru", Comments: nil},
							{ID: 1001, BookID: 100, Book: nil, Lang: "md", Comments: nil},
						},
					}, {
						ID: 101,
						Translations: []Translation{
							{ID: 1002, BookID: 101, Book: nil, Lang: "ua", Comments: nil},
						},
					}},
					ParentID: 0,
					Subgenres: []Genre{
						{ID: 3, Name: "subgenre 1", Rating: 0, Books: nil, ParentID: 1, Subgenres: nil},
						{ID: 4, Name: "subgenre 2", Rating: 0, Books: nil, ParentID: 1, Subgenres: nil},
					},
				}, {
					ID:     2,
					Name:   "genre 2",
					Rating: 0,
					Books: []Book{{
						ID: 100,
						Translations: []Translation{
							{ID: 1000, BookID: 100, Book: nil, Lang: "ru", Comments: nil},
							{ID: 1001, BookID: 100, Book: nil, Lang: "md", Comments: nil},
						},
					}},
					ParentID:  0,
					Subgenres: nil,
				},
			}))
		})

		It("fetches Translation relation", func() {
			var translations []Translation
			err := db.Model(&translations).
				Column("tr.*").
				Relation("Book.id").
				Relation("Book.Author").
				Relation("Book.Editor").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(translations).To(Equal([]Translation{{
				ID:     1000,
				BookID: 100,
				Book: &Book{
					ID:     100,
					Author: Author{ID: 10, Name: "author 1", AvatarID: 1},
					Editor: &Author{ID: 11, Name: "author 2", AvatarID: 2},
				},
				Lang: "ru",
			}, {
				ID:     1001,
				BookID: 100,
				Book: &Book{
					ID:     100,
					Author: Author{ID: 10, Name: "author 1", AvatarID: 1},
					Editor: &Author{ID: 11, Name: "author 2", AvatarID: 2},
				},
				Lang: "md",
			}, {
				ID:     1002,
				BookID: 101,
				Book: &Book{
					ID:     101,
					Author: Author{ID: 10, Name: "author 1", AvatarID: 1},
					Editor: &Author{ID: 12, Name: "author 3", AvatarID: 3},
				},
				Lang: "ua",
			}}))
		})

		It("works when there are no results", func() {
			var books []Book
			err := db.Model(&books).
				Column("book.*").
				Relation("Author").
				Relation("Genres").
				Relation("Comments").
				Where("1 = 2").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(BeNil())
		})

		It("supports overriding", func() {
			var books []BookWithCommentCount
			err := db.Model(&books).
				Column("book.id").
				Relation("Author").
				Relation("Genres").
				ColumnExpr(`(SELECT COUNT(*) FROM comments WHERE trackable_type = 'Book' AND trackable_id = book.id) AS comment_count`).
				OrderExpr("id ASC").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(Equal([]BookWithCommentCount{{
				Book: Book{
					ID:     100,
					Author: Author{ID: 10, Name: "author 1", AvatarID: 1},
					Genres: []Genre{
						{ID: 1, Name: "genre 1", Rating: 999},
						{ID: 2, Name: "genre 2", Rating: 9999},
					},
				},
				CommentCount: 2,
			}, {
				Book: Book{
					ID:     101,
					Author: Author{ID: 10, Name: "author 1", AvatarID: 1},
					Genres: []Genre{
						{ID: 1, Name: "genre 1", Rating: 99999},
					},
				},
				CommentCount: 0,
			}, {
				Book: Book{
					ID:     102,
					Author: Author{ID: 11, Name: "author 2", AvatarID: 2},
				},
				CommentCount: 0,
			}}))
		})
	})

	Describe("fetches Book relations", func() {
		It("supports HasOne, HasMany, HasMany2Many", func() {
			var books []*Book
			err := db.Model(&books).
				Column("book.id").
				Relation("Author").
				Relation("Editor").
				Relation("Translations").
				Relation("Genres").
				OrderExpr("book.id ASC").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(HaveLen(3))
		})

		It("fetches Genre relations", func() {
			var genres []*Genre
			err := db.Model(&genres).
				Column("genre.*").
				Relation("Subgenres").
				Relation("Books.id").
				Relation("Books.Translations").
				Where("genre.parent_id IS NULL").
				OrderExpr("genre.id").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(genres).To(HaveLen(2))
		})

		It("fetches Translation relations", func() {
			var translations []*Translation
			err := db.Model(&translations).
				Column("tr.*").
				Relation("Book.id").
				Relation("Book.Author").
				Relation("Book.Editor").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(translations).To(HaveLen(3))
		})

		It("works when there are no results", func() {
			var books []*Book
			err := db.Model(&books).
				Column("book.*").
				Relation("Author").
				Relation("Genres").
				Relation("Comments").
				Where("1 = 2").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(BeNil())
		})

		It("supports overriding", func() {
			var books []*BookWithCommentCount
			err := db.Model(&books).
				Column("book.id").
				Relation("Author").
				ColumnExpr(`(SELECT COUNT(*) FROM comments WHERE trackable_type = 'Book' AND trackable_id = book.id) AS comment_count`).
				OrderExpr("id ASC").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(HaveLen(3))
		})
	})

	Describe("bulk insert", func() {
		It("returns an error if there is no data", func() {
			var books []Book
			_, err := db.Model(&books).Insert()
			Expect(err).To(MatchError("pg: can't bulk-insert empty slice []pg_test.Book"))
		})

		It("inserts books", func() {
			books := []Image{{
				ID:   111,
				Path: "111.jpg",
			}, {
				ID:   222,
				Path: "222.jpg",
			}}
			_, err := db.Model(&books).Insert()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(books)).NotTo(BeZero())
		})
	})

	Describe("bulk update", func() {
		It("returns an error if there is no data", func() {
			var books []Book
			_, err := db.Model(&books).Update()
			Expect(err).To(MatchError("pg: can't bulk-update empty slice []pg_test.Book"))
		})

		It("updates books using Set", func() {
			var books []Book
			err := db.Model(&books).Order("id").Select()
			Expect(err).NotTo(HaveOccurred())

			for i := range books {
				books[i].Title = fmt.Sprintf("censored %d", i)
			}

			_, err = db.Model(&books).Set("title = ?title").Update()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(books)).NotTo(BeZero())

			books = nil
			err = db.Model(&books).Order("id").Select()
			Expect(err).NotTo(HaveOccurred())

			for i := range books {
				Expect(books[i].Title).To(Equal(fmt.Sprintf("censored %d", i)))
			}
		})

		It("updates books using Set expression", func() {
			books := []Book{{
				ID:    100,
				Title: " suffix",
			}, {
				ID: 101,
			}}
			res, err := db.Model(&books).
				Set("title = book.title || COALESCE(_data.title, '')").
				Update()
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RowsAffected()).To(Equal(2))
			Expect(len(books)).NotTo(BeZero())

			books = nil
			err = db.Model(&books).Column("id", "title").Order("id").Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(Equal([]Book{{
				ID:    100,
				Title: "book 1 suffix",
			}, {
				ID:    101,
				Title: "book 2",
			}, {
				ID:    102,
				Title: "book 3",
			}}))
		})

		It("updates books using Column", func() {
			var books []Book
			err := db.Model(&books).Order("id").Select()
			Expect(err).NotTo(HaveOccurred())

			for i := range books {
				books[i].Title = fmt.Sprintf("censored %d", i)
			}

			_, err = db.Model(&books).Column("title").Update()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(books)).NotTo(BeZero())

			books = nil
			err = db.Model(&books).Order("id").Select()
			Expect(err).NotTo(HaveOccurred())

			for i := range books {
				Expect(books[i].Title).To(Equal(fmt.Sprintf("censored %d", i)))
			}
		})
	})

	Describe("bulk delete", func() {
		It("returns an error when slice is empty", func() {
			var books []Book
			_, err := db.Model(&books).Delete()
			Expect(err).To(MatchError("pg: Update and Delete queries require Where clause (try WherePK)"))
		})

		It("deletes books", func() {
			var books []Book
			err := db.Model(&books).Order("id").Select()
			Expect(err).NotTo(HaveOccurred())

			res, err := db.Model(&books).Delete()
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RowsAffected()).To(Equal(3))

			books = make([]Book, 0)
			n, err := db.Model(&books).Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(0))
		})

		It("deletes ptrs of books", func() {
			var books []*Book
			err := db.Model(&books).Order("id").Select()
			Expect(err).NotTo(HaveOccurred())

			res, err := db.Model(&books).Delete()
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RowsAffected()).To(Equal(3))

			books = make([]*Book, 0)
			n, err := db.Model(&books).Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(0))
		})
	})

	It("filters by HasOne", func() {
		var books []Book
		err := db.Model(&books).
			Column("book.id").
			Relation("Author._").
			Where("author.id = 10").
			OrderExpr("book.id ASC").
			Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(books).To(Equal([]Book{{
			ID: 100,
		}, {
			ID: 101,
		}}))
	})

	It("supports filtering HasMany", func() {
		var book Book
		err := db.Model(&book).
			Column("book.id").
			Relation("Translations", func(q *orm.Query) (*orm.Query, error) {
				return q.Where("lang = 'ru'"), nil
			}).
			First()
		Expect(err).NotTo(HaveOccurred())
		Expect(book).To(Equal(Book{
			ID: 100,
			Translations: []Translation{
				{ID: 1000, BookID: 100, Lang: "ru"},
			},
		}))
	})

	It("supports filtering HasMany2Many", func() {
		var book Book
		err := db.Model(&book).
			Column("book.id").
			Relation("Genres", func(q *orm.Query) (*orm.Query, error) {
				return q.Where("genre__rating > 999"), nil
			}).
			First()
		Expect(err).NotTo(HaveOccurred())
		Expect(book).To(Equal(Book{
			ID: 100,
			Genres: []Genre{
				{ID: 2, Name: "genre 2", Rating: 9999},
			},
		}))
	})

	It("deletes book returning title", func() {
		book := &Book{
			ID: 100,
		}
		res, err := db.Model(book).WherePK().Returning("title").Delete()
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RowsAffected()).To(Equal(1))
		Expect(book).To(Equal(&Book{
			ID:    100,
			Title: "book 1",
		}))
	})

	It("deletes books returning id", func() {
		var ids []int
		res, err := db.Model((*Book)(nil)).Where("TRUE").Returning("id").Delete(&ids)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RowsAffected()).To(Equal(3))
		Expect(ids).To(Equal([]int{100, 101, 102}))
	})

	It("supports Exec & Query", func() {
		_, err := db.Model((*Book)(nil)).Exec("DROP TABLE ?TableName CASCADE")
		Expect(err).NotTo(HaveOccurred())

		var num int
		_, err = db.Model(&Book{}).QueryOne(pg.Scan(&num), "SELECT 1 FROM ?TableName")
		Expect(err).To(MatchError(`ERROR #42P01 relation "books" does not exist`))
	})

	It("does not create zero model for null relation", func() {
		newBook := &Book{Title: "new"}
		_, err := db.Model(newBook).Insert()
		Expect(err).NotTo(HaveOccurred())

		book := new(Book)
		err = db.Model(book).
			Relation("Editor").
			Where("book.id = ?", newBook.ID).
			Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(book.Editor).To(BeNil())
	})

	Describe("ForEach", func() {
		It("works with a struct ptr", func() {
			q := db.Model((*Book)(nil)).
				Order("id ASC")

			var books []Book
			err := q.Select(&books)
			Expect(err).NotTo(HaveOccurred())

			var count int
			err = q.ForEach(func(b *Book) error {
				book := &books[count]
				Expect(book).To(Equal(b))
				count++
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(3))
		})

		It("works with a struct", func() {
			q := db.Model((*Book)(nil)).
				Order("id ASC")

			var books []Book
			err := q.Select(&books)
			Expect(err).NotTo(HaveOccurred())

			var count int
			err = q.ForEach(func(b Book) error {
				book := &books[count]
				Expect(book).To(Equal(&b))
				count++
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(3))
		})

		It("works with a model", func() {
			q := db.Model((*Book)(nil)).
				Order("id ASC")

			var count int
			err := q.ForEach(func(_ orm.Discard) error {
				count++
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(3))
		})

		It("works with scalars", func() {
			q := db.Model((*Book)(nil)).
				ColumnExpr("id, title").
				Order("id ASC")

			var books []Book
			err := q.Select(&books)
			Expect(err).NotTo(HaveOccurred())

			var count int
			err = q.ForEach(func(id int, title string) error {
				book := &books[count]
				Expect(id).To(Equal(book.ID))
				Expect(title).To(Equal(book.Title))
				count++
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(3))
		})
	})

	Describe("SelectAndCount", func() {
		It("selects and counts books", func() {
			var books []Book
			count, err := db.Model(&books).SelectAndCount()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(3))
			Expect(books).To(HaveLen(3))
		})

		It("works with Limit=-1", func() {
			var books []Book
			count, err := db.Model(&books).Limit(-1).SelectAndCount()
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(3))
			Expect(books).To(HaveLen(0))
		})
	})

	Describe("Exists", func() {
		It("returns true for existing rows", func() {
			var books []Book
			exists, err := db.Model(&books).Exists()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(Equal(true))
			Expect(books).To(HaveLen(0))
		})

		It("returns false otherwise", func() {
			var books []Book
			exists, err := db.Model(&books).Where("id = 0").Exists()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(Equal(false))
			Expect(books).To(HaveLen(0))
		})
	})
})

type SoftDeleteWithTimeModel struct {
	ID        int
	DeletedAt time.Time `pg:",soft_delete"`
}

var _ = Describe("soft delete with time column", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = testDB()

		err := db.Model((*SoftDeleteWithTimeModel)(nil)).CreateTable(&orm.CreateTableOptions{
			Temp: true,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err := db.Model((*SoftDeleteWithTimeModel)(nil)).DropTable(nil)
		Expect(err).NotTo(HaveOccurred())
	})

	assert := func() {
		It("soft deletes the model", func() {
			model := new(SoftDeleteWithTimeModel)
			err := db.Model(model).Select()
			Expect(err).To(Equal(pg.ErrNoRows))

			n, err := db.Model((*SoftDeleteWithTimeModel)(nil)).Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(0))
		})

		It("Deleted allows to select deleted model", func() {
			model := new(SoftDeleteWithTimeModel)
			err := db.Model(model).Deleted().Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(model.ID).To(Equal(1))
			Expect(model.DeletedAt).To(BeTemporally("~", time.Now(), 3*time.Second))

			n, err := db.Model((*SoftDeleteWithTimeModel)(nil)).Deleted().Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(1))
		})

		Describe("ForceDelete", func() {
			BeforeEach(func() {
				model := &SoftDeleteWithTimeModel{
					ID: 1,
				}
				_, err := db.Model(model).WherePK().ForceDelete()
				Expect(err).NotTo(HaveOccurred())
			})

			It("deletes the model", func() {
				model := new(SoftDeleteWithTimeModel)
				err := db.Model(model).Deleted().Select()
				Expect(err).To(Equal(pg.ErrNoRows))

				n, err := db.Model((*SoftDeleteWithTimeModel)(nil)).Deleted().Count()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(0))
			})
		})
	}

	Describe("nil model", func() {
		BeforeEach(func() {
			model := &SoftDeleteWithTimeModel{
				ID: 1,
			}
			_, err := db.Model(model).Insert()
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Model((*SoftDeleteWithTimeModel)(nil)).Where("1 = 1").Delete()
			Expect(err).NotTo(HaveOccurred())
		})

		assert()
	})

	Describe("model", func() {
		BeforeEach(func() {
			model := &SoftDeleteWithTimeModel{
				ID: 1,
			}
			_, err := db.Model(model).Insert()
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Model(model).WherePK().Delete()
			Expect(err).NotTo(HaveOccurred())
			Expect(model.DeletedAt).To(BeTemporally("~", time.Now(), time.Second))
		})

		assert()
	})
})

type SoftDeleteWithIntModel struct {
	ID        int
	DeletedAt *int64 `pg:",soft_delete"`
}

var _ = Describe("soft delete with int column", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = testDB()

		err := db.Model((*SoftDeleteWithIntModel)(nil)).CreateTable(&orm.CreateTableOptions{
			Temp: true,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err := db.Model((*SoftDeleteWithIntModel)(nil)).DropTable(nil)
		Expect(err).NotTo(HaveOccurred())
	})

	assert := func() {
		It("soft deletes the model", func() {
			model := new(SoftDeleteWithIntModel)
			err := db.Model(model).Select()
			Expect(err).To(Equal(pg.ErrNoRows))

			n, err := db.Model((*SoftDeleteWithIntModel)(nil)).Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(0))
		})

		It("Deleted allows to select deleted model", func() {
			model := new(SoftDeleteWithIntModel)
			err := db.Model(model).Deleted().Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(model.ID).To(Equal(1))
			deletedTime := time.Unix(0, *model.DeletedAt)
			Expect(deletedTime).To(BeTemporally("~", time.Now(), time.Second))

			n, err := db.Model((*SoftDeleteWithIntModel)(nil)).Deleted().Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(1))
		})

		Describe("ForceDelete", func() {
			BeforeEach(func() {
				model := &SoftDeleteWithIntModel{
					ID: 1,
				}
				_, err := db.Model(model).WherePK().ForceDelete()
				Expect(err).NotTo(HaveOccurred())
			})

			It("deletes the model", func() {
				model := new(SoftDeleteWithIntModel)
				err := db.Model(model).Deleted().Select()
				Expect(err).To(Equal(pg.ErrNoRows))

				n, err := db.Model((*SoftDeleteWithIntModel)(nil)).Deleted().Count()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(0))
			})
		})
	}

	Describe("nil model", func() {
		BeforeEach(func() {
			model := &SoftDeleteWithIntModel{
				ID: 1,
			}
			_, err := db.Model(model).Insert()
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Model((*SoftDeleteWithIntModel)(nil)).Where("1 = 1").Delete()
			Expect(err).NotTo(HaveOccurred())
		})

		assert()
	})

	Describe("model", func() {
		BeforeEach(func() {
			model := &SoftDeleteWithIntModel{
				ID: 1,
			}
			_, err := db.Model(model).Insert()
			Expect(err).NotTo(HaveOccurred())

			_, err = db.Model(model).WherePK().Delete()
			Expect(err).NotTo(HaveOccurred())
			deletedTime := time.Unix(0, *model.DeletedAt)
			Expect(deletedTime).To(BeTemporally("~", time.Now(), time.Second))
		})

		assert()
	})
})
