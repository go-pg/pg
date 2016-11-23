package pg_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/pg.v5"
	"gopkg.in/pg.v5/orm"
)

func init() {
	//pg.SetLogger(log.New(os.Stderr, "pg: ", log.LstdFlags))
}

func TestGinkgo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pg")
}

func pgOptions() *pg.Options {
	return &pg.Options{
		User:               "postgres",
		Database:           "postgres",
		DialTimeout:        30 * time.Second,
		ReadTimeout:        10 * time.Second,
		WriteTimeout:       10 * time.Second,
		PoolSize:           10,
		PoolTimeout:        30 * time.Second,
		IdleTimeout:        10 * time.Second,
		IdleCheckFrequency: 100 * time.Millisecond,
	}
}

func TestDBString(t *testing.T) {
	db := pg.Connect(pgOptions())
	wanted := `DB<Addr="localhost:5432">`
	if db.String() != wanted {
		t.Fatalf("got %q, wanted %q", db.String(), wanted)
	}

	db = db.WithParam("param1", "value1").WithParam("param2", 2)
	wanted = `DB<Addr="localhost:5432" param1=value1 param2=2>`
	if db.String() != wanted {
		t.Fatalf("got %q, wanted %q", db.String(), wanted)
	}
}

var _ = Describe("Time", func() {
	var tests = []struct {
		str    string
		wanted time.Time
	}{
		{"0001-01-01 00:00:00+00", time.Time{}},
		{"0000-01-01 00:00:00+00", time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC)},

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
			Expect(tm.Unix()).To(Equal(test.wanted.Unix()), "#%d str=%q wanted=%q", i, test.str, test.wanted)
		}
	})

	It("is parsed correctly", func() {
		for i, test := range tests {
			var tm time.Time
			_, err := db.QueryOne(pg.Scan(&tm), "SELECT ?", test.str)
			Expect(err).NotTo(HaveOccurred())
			Expect(tm.Unix()).To(Equal(test.wanted.Unix()), "#%d str=%q wanted=%q", i, test.str, test.wanted)
		}
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
		var ints []int
		_, err := db.Query(&ints, "SELECT generate_series(1, 0)")
		Expect(err).NotTo(HaveOccurred())
		Expect(ints).To(BeZero())
	})

	It("does not error when there are no rows", func() {
		var slice []value
		_, err := db.Query(&slice, "SELECT generate_series(1, 0)")
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(BeZero())
	})

	It("does not error when there are no rows", func() {
		var slice []*value
		_, err := db.Query(&slice, "SELECT generate_series(1, 0)")
		Expect(err).NotTo(HaveOccurred())
		Expect(slice).To(BeZero())
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
			"CREATE TEMP TABLE copy_from(n int)",
			"CREATE TEMP TABLE copy_to(n int)",
			fmt.Sprintf("INSERT INTO copy_from SELECT generate_series(1, %d)", n),
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
		res, err := db.CopyTo(&buf, "COPY copy_from TO STDOUT")
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RowsAffected()).To(Equal(n))

		res, err = db.CopyFrom(&buf, "COPY copy_to FROM STDIN")
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RowsAffected()).To(Equal(n))

		var count int
		_, err = db.QueryOne(pg.Scan(&count), "SELECT count(*) FROM copy_to")
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(n))

		st := db.Pool().Stats()
		Expect(st.Requests).To(Equal(uint32(6)))
		Expect(st.Hits).To(Equal(uint32(5)))
		Expect(st.Timeouts).To(Equal(uint32(0)))
		Expect(st.TotalConns).To(Equal(uint32(1)))
		Expect(st.FreeConns).To(Equal(uint32(1)))
	})

	It("copies corrupted data to a table", func() {
		buf := bytes.NewBufferString("corrupted data")
		res, err := db.CopyFrom(buf, "COPY copy_to FROM STDIN")
		Expect(err).To(MatchError(`ERROR #22P02 invalid input syntax for integer: "corrupted data" (addr="127.0.0.1:5432")`))
		Expect(res).To(BeNil())

		st := db.Pool().Stats()
		Expect(st.Requests).To(Equal(uint32(4)))
		Expect(st.Hits).To(Equal(uint32(3)))
		Expect(st.Timeouts).To(Equal(uint32(0)))
		Expect(st.TotalConns).To(Equal(uint32(1)))
		Expect(st.FreeConns).To(Equal(uint32(1)))
	})
})

var _ = Describe("CountEstimate", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
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
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("sql.NullInt64", func() {
		type Test struct {
			Id    int
			Value sql.NullInt64
		}

		It("inserts null value", func() {
			ins := Test{
				Id: 1,
			}
			err := db.Insert(&ins)
			Expect(err).NotTo(HaveOccurred())

			sel := Test{
				Id: 1,
			}
			err = db.Select(&sel)
			Expect(err).NotTo(HaveOccurred())
			Expect(sel.Value.Valid).To(BeFalse())
		})

		It("inserts non-null value", func() {
			ins := Test{
				Id: 1,
				Value: sql.NullInt64{
					Int64: 2,
					Valid: true,
				},
			}
			err := db.Insert(&ins)
			Expect(err).NotTo(HaveOccurred())

			sel := Test{
				Id: 1,
			}
			err = db.Select(&sel)
			Expect(err).NotTo(HaveOccurred())
			Expect(sel.Value.Valid).To(BeTrue())
			Expect(sel.Value.Int64).To(Equal(int64(2)))
		})
	})

	Context("nil ptr", func() {
		type Test struct {
			Id    int
			Value *int
		}

		It("inserts null value", func() {
			ins := Test{
				Id: 1,
			}
			err := db.Insert(&ins)
			Expect(err).NotTo(HaveOccurred())

			sel := Test{
				Id: 1,
			}
			err = db.Select(&sel)
			Expect(err).NotTo(HaveOccurred())
			Expect(sel.Value).To(BeNil())
		})

		It("inserts non-null value", func() {
			value := 2
			ins := Test{
				Id:    1,
				Value: &value,
			}
			err := db.Insert(&ins)
			Expect(err).NotTo(HaveOccurred())

			sel := Test{
				Id: 1,
			}
			err = db.Select(&sel)
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

		qs := []string{
			`CREATE TEMP TABLE tests (col bytea)`,
			fmt.Sprintf(`INSERT INTO tests VALUES ('\x%x')`, []byte("bytes")),
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

	It("selects bytea", func() {
		var col []byte
		err := db.Model().Table("tests").Column("col").Select(pg.Scan(&col))
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("DB.Insert", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns an error on nil", func() {
		err := db.Insert(nil)
		Expect(err).To(MatchError("pg: Model(nil)"))
	})

	It("returns an errors if value is not settable", func() {
		err := db.Insert(1)
		Expect(err).To(MatchError("pg: Model(non-pointer int)"))
	})

	It("returns an errors if value is not supported", func() {
		var v int
		err := db.Insert(&v)
		Expect(err).To(MatchError("pg: Model(unsupported int)"))
	})
})

var _ = Describe("DB.Update", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns an error on nil", func() {
		err := db.Update(nil)
		Expect(err).To(MatchError("pg: Model(nil)"))
	})

	It("returns an error if there are no pks", func() {
		type Test struct{}
		var test Test
		err := db.Update(&test)
		Expect(err).To(MatchError(`model=Test does not have primary keys`))
	})
})

var _ = Describe("DB.Delete", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns an error on nil", func() {
		err := db.Delete(nil)
		Expect(err).To(MatchError("pg: Model(nil)"))
	})

	It("returns an error if there are no pks", func() {
		type Test struct{}
		var test Test
		err := db.Delete(&test)
		Expect(err).To(MatchError(`model=Test does not have primary keys`))
	})
})

var _ = Describe("errors", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("unknown column error", func() {
		type Test struct {
			Col1 int
		}

		var test Test
		_, err := db.QueryOne(&test, "SELECT 1 AS col1, 2 AS col2")
		Expect(err).To(MatchError("pg: can't find column=col2 in model=Test"))
		Expect(test.Col1).To(Equal(1))
	})

	It("Scan error", func() {
		var n1 int
		_, err := db.QueryOne(pg.Scan(&n1), "SELECT 1, 2")
		Expect(err).To(MatchError("pg: no Scan value for column index=1 name=?column?"))
		Expect(n1).To(Equal(1))
	})
})

type Genre struct {
	// TableName is an optional field that specifies custom table name and alias.
	// By default go-pg generates table name and alias from struct name.
	TableName struct{} `sql:"genres,alias:genre"` // default values are the same

	Id     int // Id is automatically detected as primary key
	Name   string
	Rating int `sql:"-"` // - is used to ignore field

	Books []Book `pg:",many2many:book_genres"` // many to many relation

	ParentId  int
	Subgenres []Genre `pg:",fk:Parent"` // fk specifies prefix for foreign key (ParentId)
}

func (g Genre) String() string {
	return fmt.Sprintf("Genre<Id=%d Name=%q>", g.Id, g.Name)
}

type Author struct {
	ID    int // both "Id" and "ID" are detected as primary key
	Name  string
	Books []*Book // has many relation
}

func (a Author) String() string {
	return fmt.Sprintf("Author<ID=%d Name=%q>", a.ID, a.Name)
}

type BookGenre struct {
	TableName struct{} `sql:",alias:bg"` // custom table alias

	BookId  int `sql:",pk"` // pk tag is used to mark field as primary key
	GenreId int `sql:",pk"`

	Genre_Rating int // belongs to and is copied to Genre model
}

type Book struct {
	Id        int
	Title     string
	AuthorID  int
	Author    *Author // has one relation
	EditorID  int
	Editor    *Author // has one relation
	CreatedAt time.Time

	Genres       []Genre       `pg:",many2many:book_genres" gorm:"many2many:book_genres;"` // many to many relation
	Translations []Translation // has many relation
	Comments     []Comment     `pg:",polymorphic:Trackable"` // has many polymorphic relation
}

func (b Book) String() string {
	return fmt.Sprintf("Book<Id=%d Title=%q>", b.Id, b.Title)
}

func (b *Book) BeforeInsert(db orm.DB) error {
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now()
	}
	return nil
}

// BookWithCommentCount is like Book model, but has additional CommentCount
// field that is used to select data into it. The use of `pg:",override"` tag
// is essential here and it overrides internal model properties such as table name.
type BookWithCommentCount struct {
	Book `pg:",override"`

	CommentCount int
}

type Translation struct {
	TableName struct{} `sql:",alias:tr"` // custom table alias

	Id     int
	BookId int
	Book   *Book // belongs to relation
	Lang   string

	Comments []Comment `pg:",polymorphic:Trackable"` // has many polymorphic relation
}

type Comment struct {
	TrackableId   int    // Book.Id or Translation.Id
	TrackableType string // "Book" or "Translation"
	Text          string
}

func createTestSchema(db *pg.DB) error {
	sql := []string{
		`DROP TABLE IF EXISTS comments`,
		`DROP TABLE IF EXISTS translations`,
		`DROP TABLE IF EXISTS authors`,
		`DROP TABLE IF EXISTS books`,
		`DROP TABLE IF EXISTS genres`,
		`DROP TABLE IF EXISTS book_genres`,
	}
	for _, q := range sql {
		_, err := db.Exec(q)
		if err != nil {
			return err
		}
	}

	tables := []interface{}{
		&Author{},
		&Book{},
		&Genre{},
		&BookGenre{},
		&Translation{},
		&Comment{},
	}
	for _, table := range tables {
		err := db.CreateTable(table, nil)
		if err != nil {
			return err
		}
	}

	_, err := db.Exec(`CREATE UNIQUE INDEX authors_name ON authors (name)`)
	return err
}

var _ = Describe("ORM", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())

		err := createTestSchema(db)
		Expect(err).NotTo(HaveOccurred())

		genres := []Genre{{
			Id:   1,
			Name: "genre 1",
		}, {
			Id:   2,
			Name: "genre 2",
		}, {
			Id:       3,
			Name:     "subgenre 1",
			ParentId: 1,
		}, {
			Id:       4,
			Name:     "subgenre 2",
			ParentId: 1,
		}}

		err = db.Insert(&genres)
		Expect(err).NotTo(HaveOccurred())
		Expect(genres).To(HaveLen(4))

		authors := []Author{{
			ID:   10,
			Name: "author 1",
		}, {
			ID:   11,
			Name: "author 2",
		}, Author{
			ID:   12,
			Name: "author 3",
		}}
		err = db.Insert(&authors)
		Expect(err).NotTo(HaveOccurred())
		Expect(authors).To(HaveLen(3))

		books := []Book{{
			Id:       100,
			Title:    "book 1",
			AuthorID: 10,
			EditorID: 11,
		}, {
			Id:       101,
			Title:    "book 2",
			AuthorID: 10,
			EditorID: 12,
		}, Book{
			Id:       102,
			Title:    "book 3",
			AuthorID: 11,
			EditorID: 11,
		}}
		err = db.Insert(&books)
		Expect(err).NotTo(HaveOccurred())
		Expect(books).To(HaveLen(3))
		for _, book := range books {
			Expect(book.CreatedAt).To(BeTemporally("~", time.Now(), time.Second))
		}

		bookGenres := []BookGenre{{
			BookId:       100,
			GenreId:      1,
			Genre_Rating: 999,
		}, {
			BookId:       100,
			GenreId:      2,
			Genre_Rating: 9999,
		}, {
			BookId:       101,
			GenreId:      1,
			Genre_Rating: 99999,
		}}
		err = db.Insert(&bookGenres)
		Expect(err).NotTo(HaveOccurred())
		Expect(bookGenres).To(HaveLen(3))

		translations := []Translation{{
			Id:     1000,
			BookId: 100,
			Lang:   "ru",
		}, {
			Id:     1001,
			BookId: 100,
			Lang:   "md",
		}, {
			Id:     1002,
			BookId: 101,
			Lang:   "ua",
		}}
		err = db.Insert(&translations)
		Expect(err).NotTo(HaveOccurred())
		Expect(translations).To(HaveLen(3))

		comments := []Comment{{
			TrackableId:   100,
			TrackableType: "Book",
			Text:          "comment1",
		}, {
			TrackableId:   100,
			TrackableType: "Book",
			Text:          "comment2",
		}, {
			TrackableId:   1000,
			TrackableType: "Translation",
			Text:          "comment3",
		}}
		err = db.Insert(&comments)
		Expect(err).NotTo(HaveOccurred())
		Expect(comments).To(HaveLen(3))
	})

	Describe("struct model", func() {
		It("supports HasOne, HasMany, HasMany2Many, Polymorphic, HasMany -> Polymorphic", func() {
			var book Book
			err := db.Model(&book).
				Column("book.id", "Author", "Editor", "Genres", "Comments", "Translations", "Translations.Comments").
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(book).To(Equal(Book{
				Id:        100,
				Title:     "",
				Author:    &Author{ID: 10, Name: "author 1", Books: nil},
				Editor:    &Author{ID: 11, Name: "author 2", Books: nil},
				CreatedAt: time.Time{},
				Genres: []Genre{
					{Id: 1, Name: "genre 1", Rating: 999},
					{Id: 2, Name: "genre 2", Rating: 9999},
				},
				Translations: []Translation{{
					Id:     1000,
					BookId: 100,
					Lang:   "ru",
					Comments: []Comment{
						{TrackableId: 1000, TrackableType: "Translation", Text: "comment3"},
					},
				}, {
					Id:       1001,
					BookId:   100,
					Lang:     "md",
					Comments: nil,
				}},
				Comments: []Comment{
					{TrackableId: 100, TrackableType: "Book", Text: "comment1"},
					{TrackableId: 100, TrackableType: "Book", Text: "comment2"},
				},
			}))
		})

		It("supports HasMany -> HasOne, HasMany -> HasMany", func() {
			var author Author
			err := db.Model(&author).
				Column(
					"author.*",
					"Books.id", "Books.author_id", "Books.editor_id",
					"Books.Author", "Books.Editor",
					"Books.Translations",
				).
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(author).To(Equal(Author{
				ID:   10,
				Name: "author 1",
				Books: []*Book{{
					Id:        100,
					Title:     "",
					AuthorID:  10,
					Author:    &Author{ID: 10, Name: "author 1", Books: nil},
					EditorID:  11,
					Editor:    &Author{ID: 11, Name: "author 2", Books: nil},
					CreatedAt: time.Time{},
					Genres:    nil,
					Translations: []Translation{
						{Id: 1000, BookId: 100, Book: nil, Lang: "ru", Comments: nil},
						{Id: 1001, BookId: 100, Book: nil, Lang: "md", Comments: nil},
					},
				}, {
					Id:        101,
					Title:     "",
					AuthorID:  10,
					Author:    &Author{ID: 10, Name: "author 1", Books: nil},
					EditorID:  12,
					Editor:    &Author{ID: 12, Name: "author 3", Books: nil},
					CreatedAt: time.Time{},
					Genres:    nil,
					Translations: []Translation{
						{Id: 1002, BookId: 101, Book: nil, Lang: "ua", Comments: nil},
					},
				}},
			}))
		})

		It("supports HasMany -> HasMany -> HasMany", func() {
			var genre Genre
			err := db.Model(&genre).
				Column("genre.*", "Books.id", "Books.Translations").
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(genre).To(Equal(Genre{
				Id:     1,
				Name:   "genre 1",
				Rating: 0,
				Books: []Book{{
					Id:        100,
					Title:     "",
					AuthorID:  0,
					Author:    nil,
					EditorID:  0,
					Editor:    nil,
					CreatedAt: time.Time{},
					Genres:    nil,
					Translations: []Translation{
						{Id: 1000, BookId: 100, Book: nil, Lang: "ru", Comments: nil},
						{Id: 1001, BookId: 100, Book: nil, Lang: "md", Comments: nil},
					},
					Comments: nil,
				}, {
					Id:        101,
					Title:     "",
					AuthorID:  0,
					Author:    nil,
					EditorID:  0,
					Editor:    nil,
					CreatedAt: time.Time{},
					Genres:    nil,
					Translations: []Translation{
						{Id: 1002, BookId: 101, Book: nil, Lang: "ua", Comments: nil},
					},
					Comments: nil,
				}},
				ParentId:  0,
				Subgenres: nil,
			}))
		})

		It("supports HasOne -> HasOne", func() {
			var translation Translation
			err := db.Model(&translation).
				Column("tr.*", "Book.id", "Book.Author", "Book.Editor").
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(translation).To(Equal(Translation{
				Id:     1000,
				BookId: 100,
				Book: &Book{
					Id:     100,
					Author: &Author{ID: 10, Name: "author 1"},
					Editor: &Author{ID: 11, Name: "author 2"},
				},
				Lang: "ru",
			}))
		})

		It("works when there are no results", func() {
			var book Book
			err := db.Model(&book).
				Column("book.*", "Author", "Genres", "Comments").
				Where("1 = 2").
				Select()
			Expect(err).To(Equal(pg.ErrNoRows))
		})

		It("supports overriding", func() {
			var book BookWithCommentCount
			err := db.Model(&book).
				Column("book.id", "Author").
				ColumnExpr(`(SELECT COUNT(*) FROM comments WHERE trackable_type = 'Book' AND trackable_id = book.id) AS comment_count`).
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(book).To(Equal(BookWithCommentCount{
				Book: Book{
					Id:     100,
					Author: &Author{ID: 10, Name: "author 1"},
				},
				CommentCount: 2,
			}))
		})
	})

	Describe("slice model", func() {
		It("supports HasOne, HasMany, HasMany2Many", func() {
			var books []Book
			err := db.Model(&books).
				Column("book.id", "Author", "Editor", "Translations", "Genres").
				OrderExpr("book.id ASC").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(Equal([]Book{{
				Id:        100,
				Title:     "",
				AuthorID:  0,
				Author:    &Author{ID: 10, Name: "author 1", Books: nil},
				EditorID:  0,
				Editor:    &Author{ID: 11, Name: "author 2", Books: nil},
				CreatedAt: time.Time{},
				Genres: []Genre{
					{Id: 1, Name: "genre 1", Rating: 999, Books: nil, ParentId: 0, Subgenres: nil},
					{Id: 2, Name: "genre 2", Rating: 9999, Books: nil, ParentId: 0, Subgenres: nil},
				},
				Translations: []Translation{
					{Id: 1000, BookId: 100, Book: nil, Lang: "ru", Comments: nil},
					{Id: 1001, BookId: 100, Book: nil, Lang: "md", Comments: nil},
				},
				Comments: nil,
			}, {
				Id:        101,
				Title:     "",
				AuthorID:  0,
				Author:    &Author{ID: 10, Name: "author 1", Books: nil},
				EditorID:  0,
				Editor:    &Author{ID: 12, Name: "author 3", Books: nil},
				CreatedAt: time.Time{},
				Genres: []Genre{
					{Id: 1, Name: "genre 1", Rating: 99999, Books: nil, ParentId: 0, Subgenres: nil},
				},
				Translations: []Translation{
					{Id: 1002, BookId: 101, Book: nil, Lang: "ua", Comments: nil},
				},
				Comments: nil,
			}, {
				Id:           102,
				Title:        "",
				AuthorID:     0,
				Author:       &Author{ID: 11, Name: "author 2", Books: nil},
				EditorID:     0,
				Editor:       &Author{ID: 11, Name: "author 2", Books: nil},
				CreatedAt:    time.Time{},
				Genres:       nil,
				Translations: nil,
				Comments:     nil,
			}}))
		})

		It("supports HasMany2Many, HasMany2Many -> HasMany", func() {
			var genres []Genre
			err := db.Model(&genres).
				Column("genre.*", "Subgenres", "Books.id", "Books.Translations").
				Where("genre.parent_id IS NULL").
				OrderExpr("genre.id").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(genres).To(Equal([]Genre{{
				Id:     1,
				Name:   "genre 1",
				Rating: 0,
				Books: []Book{{
					Id:        100,
					Title:     "",
					AuthorID:  0,
					Author:    nil,
					EditorID:  0,
					Editor:    nil,
					CreatedAt: time.Time{},
					Genres:    nil,
					Translations: []Translation{
						{Id: 1000, BookId: 100, Book: nil, Lang: "ru", Comments: nil},
						{Id: 1001, BookId: 100, Book: nil, Lang: "md", Comments: nil},
					},
					Comments: nil,
				}, {
					Id:        101,
					Title:     "",
					AuthorID:  0,
					Author:    nil,
					EditorID:  0,
					Editor:    nil,
					CreatedAt: time.Time{},
					Genres:    nil,
					Translations: []Translation{
						{Id: 1002, BookId: 101, Book: nil, Lang: "ua", Comments: nil},
					},
					Comments: nil,
				}},
				ParentId: 0,
				Subgenres: []Genre{
					{Id: 3, Name: "subgenre 1", Rating: 0, Books: nil, ParentId: 1, Subgenres: nil},
					{Id: 4, Name: "subgenre 2", Rating: 0, Books: nil, ParentId: 1, Subgenres: nil},
				},
			}, {
				Id:     2,
				Name:   "genre 2",
				Rating: 0,
				Books: []Book{{
					Id:        100,
					Title:     "",
					AuthorID:  0,
					Author:    nil,
					EditorID:  0,
					Editor:    nil,
					CreatedAt: time.Time{},
					Genres:    nil,
					Translations: []Translation{
						{Id: 1000, BookId: 100, Book: nil, Lang: "ru", Comments: nil},
						{Id: 1001, BookId: 100, Book: nil, Lang: "md", Comments: nil},
					},
					Comments: nil,
				}},
				ParentId:  0,
				Subgenres: nil,
			},
			}))
		})

		It("supports HasOne -> HasOne", func() {
			var translations []Translation
			err := db.Model(&translations).
				Column("tr.*", "Book.id", "Book.Author", "Book.Editor").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(translations).To(Equal([]Translation{{
				Id:     1000,
				BookId: 100,
				Book: &Book{
					Id:     100,
					Author: &Author{ID: 10, Name: "author 1"},
					Editor: &Author{ID: 11, Name: "author 2"},
				},
				Lang: "ru",
			}, {
				Id:     1001,
				BookId: 100,
				Book: &Book{
					Id:     100,
					Author: &Author{ID: 10, Name: "author 1"},
					Editor: &Author{ID: 11, Name: "author 2"},
				},
				Lang: "md",
			}, {
				Id:     1002,
				BookId: 101,
				Book: &Book{
					Id:     101,
					Author: &Author{ID: 10, Name: "author 1", Books: nil},
					Editor: &Author{ID: 12, Name: "author 3", Books: nil},
				},
				Lang: "ua",
			}}))
		})

		It("works when there are no results", func() {
			var books []Book
			err := db.Model(&books).
				Column("book.*", "Author", "Genres", "Comments").
				Where("1 = 2").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(BeNil())
		})

		It("supports overriding", func() {
			var books []BookWithCommentCount
			err := db.Model(&books).
				Column("book.id", "Author").
				ColumnExpr(`(SELECT COUNT(*) FROM comments WHERE trackable_type = 'Book' AND trackable_id = book.id) AS comment_count`).
				OrderExpr("id ASC").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(Equal([]BookWithCommentCount{{
				Book: Book{
					Id:     100,
					Author: &Author{ID: 10, Name: "author 1", Books: nil},
				},
				CommentCount: 2,
			}, {
				Book: Book{
					Id:     101,
					Author: &Author{ID: 10, Name: "author 1", Books: nil},
				},
				CommentCount: 0,
			}, {
				Book: Book{
					Id:     102,
					Author: &Author{ID: 11, Name: "author 2", Books: nil},
				},
				CommentCount: 0,
			}}))
		})
	})

	Describe("slice of ptrs model", func() {
		It("supports HasOne, HasMany, HasMany2Many", func() {
			var books []*Book
			err := db.Model(&books).
				Column("book.id", "Author", "Editor", "Translations", "Genres").
				OrderExpr("book.id ASC").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(HaveLen(3))
		})

		It("supports HasMany2Many, HasMany2Many -> HasMany", func() {
			var genres []*Genre
			err := db.Model(&genres).
				Column("genre.*", "Subgenres", "Books.id", "Books.Translations").
				Where("genre.parent_id IS NULL").
				OrderExpr("genre.id").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(genres).To(HaveLen(2))
		})

		It("supports HasOne -> HasOne", func() {
			var translations []*Translation
			err := db.Model(&translations).
				Column("tr.*", "Book.id", "Book.Author", "Book.Editor").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(translations).To(HaveLen(3))
		})

		It("works when there are no results", func() {
			var books []*Book
			err := db.Model(&books).
				Column("book.*", "Author", "Genres", "Comments").
				Where("1 = 2").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(BeNil())
		})

		It("supports overriding", func() {
			var books []*BookWithCommentCount
			err := db.Model(&books).
				Column("book.id", "Author").
				ColumnExpr(`(SELECT COUNT(*) FROM comments WHERE trackable_type = 'Book' AND trackable_id = book.id) AS comment_count`).
				OrderExpr("id ASC").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(HaveLen(3))
		})
	})

	It("filters by HasOne", func() {
		var books []Book
		err := db.Model(&books).
			Column("book.id", "Author._").
			Where("author.id = 10").
			OrderExpr("book.id ASC").
			Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(books).To(Equal([]Book{{
			Id:           100,
			Title:        "",
			AuthorID:     0,
			Author:       nil,
			EditorID:     0,
			Editor:       nil,
			CreatedAt:    time.Time{},
			Genres:       nil,
			Translations: nil,
			Comments:     nil,
		}, {
			Id:           101,
			Title:        "",
			AuthorID:     0,
			Author:       nil,
			EditorID:     0,
			Editor:       nil,
			CreatedAt:    time.Time{},
			Genres:       nil,
			Translations: nil,
			Comments:     nil,
		}}))
	})

	It("supports filtering HasMany", func() {
		var book Book
		err := db.Model(&book).
			Column("book.id", "Translations").
			Relation("Translations", func(q *orm.Query) (*orm.Query, error) {
				return q.Where("lang = 'ru'"), nil
			}).
			First()
		Expect(err).NotTo(HaveOccurred())
		Expect(book).To(Equal(Book{
			Id: 100,
			Translations: []Translation{
				{Id: 1000, BookId: 100, Lang: "ru"},
			},
		}))
	})

	It("supports filtering HasMany2Many", func() {
		var book Book
		err := db.Model(&book).
			Column("book.id", "Genres").
			Relation("Genres", func(q *orm.Query) (*orm.Query, error) {
				return q.Where("genre__rating > 999"), nil
			}).
			First()
		Expect(err).NotTo(HaveOccurred())
		Expect(book).To(Equal(Book{
			Id: 100,
			Genres: []Genre{
				{Id: 2, Name: "genre 2", Rating: 9999},
			},
		}))
	})
})
