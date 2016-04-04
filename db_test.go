package pg_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/pg.v4"
)

func TestPG(t *testing.T) {
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
		IdleTimeout:        time.Second,
		IdleCheckFrequency: time.Second,
	}
}

var _ = Describe("Collection", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("supports slice of structs", func() {
		coll := []struct {
			Id int
		}{}
		_, err := db.Query(&coll, `
			WITH data (id) AS (VALUES (1), (2), (3))
			SELECT id FROM data
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(coll).To(HaveLen(3))
		Expect(coll[0].Id).To(Equal(1))
		Expect(coll[1].Id).To(Equal(2))
		Expect(coll[2].Id).To(Equal(3))
	})

	It("supports slice of pointers", func() {
		coll := []*struct {
			Id int
		}{}
		_, err := db.Query(&coll, `
			WITH data (id) AS (VALUES (1), (2), (3))
			SELECT id FROM data
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(coll).To(HaveLen(3))
		Expect(coll[0].Id).To(Equal(1))
		Expect(coll[1].Id).To(Equal(2))
		Expect(coll[2].Id).To(Equal(3))
	})

	It("supports Collection interface", func() {
		var coll pg.Ints
		_, err := db.Query(&coll, `
			WITH data (id) AS (VALUES (1), (2), (3))
			SELECT id FROM data
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(coll).To(HaveLen(3))
		Expect(coll[0]).To(Equal(int64(1)))
		Expect(coll[1]).To(Equal(int64(2)))
		Expect(coll[2]).To(Equal(int64(3)))
	})

	It("supports slice of values", func() {
		var ints []int
		_, err := db.Query(&ints, `
			WITH data (id) AS (VALUES (1), (2), (3))
			SELECT id FROM data
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(ints).To(HaveLen(3))
		Expect(ints[0]).To(Equal(1))
		Expect(ints[1]).To(Equal(2))
		Expect(ints[2]).To(Equal(3))
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

	Describe("WithTimeout", func() {
		It("slow query passes", func() {
			_, err := db.WithTimeout(time.Minute).Exec(`SELECT pg_sleep(1)`)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("DB.Create", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns an error on nil", func() {
		err := db.Create(nil)
		Expect(err).To(MatchError("pg: NewModel(nil)"))
	})

	It("returns an errors if value is not settable", func() {
		err := db.Create(1)
		Expect(err).To(MatchError("pg: NewModel(unsupported int)"))
	})

	It("returns an errors if value is not supported", func() {
		var v int
		err := db.Create(&v)
		Expect(err).To(MatchError("pg: NewModel(unsupported int)"))
	})
})

type Genre struct {
	TableName struct{} `sql:"genres"` // specifies custom table name

	Id     int // Id is automatically detected as primary key
	Name   string
	Rating int `sql:"-"` // - is used to ignore field

	Books []Book `pg:",many2many:book_genres"` // many to many relation
}

func (g Genre) String() string {
	return fmt.Sprintf("Genre<Id=%d Name=%q>", g.Id, g.Name)
}

type Author struct {
	ID    int // both "Id" and "ID" are detected as primary key
	Name  string
	Books []Book // has many relation
}

func (a Author) String() string {
	return fmt.Sprintf("Author<ID=%d Name=%q>", a.ID, a.Name)
}

type BookGenre struct {
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

type Translation struct {
	Id     int
	BookId int
	Book   *Book // belongs to relation
	Lang   string

	Comments []Comment `pg:",polymorphic:Trackable"` // has many polymorphic relation
}

type Comment struct {
	TrackableId   int    `sql:",pk"` // Book.Id or Translation.Id
	TrackableType string `sql:",pk"` // "book" or "translation"
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
		`CREATE TABLE authors (id serial, name text)`,
		`CREATE TABLE books (id serial, title text, author_id int, editor_id int, created_at timestamptz)`,
		`CREATE TABLE genres (id serial, name text)`,
		`CREATE TABLE book_genres (book_id int, genre_id int, genre__rating int)`,
		`CREATE TABLE translations (id serial, book_id int, lang varchar(2))`,
		`CREATE TABLE comments (trackable_id int, trackable_type varchar(100), text text)`,
	}
	for _, q := range sql {
		_, err := db.Exec(q)
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

		err = db.Create(&Genre{
			Id:   1,
			Name: "genre 1",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Genre{
			Id:   2,
			Name: "genre 2",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Author{
			ID:   10,
			Name: "author 1",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Author{
			ID:   11,
			Name: "author 2",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Author{
			ID:   12,
			Name: "author 3",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Book{
			Id:        100,
			Title:     "book 1",
			AuthorID:  10,
			EditorID:  11,
			CreatedAt: time.Now(),
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Book{
			Id:        101,
			Title:     "book 2",
			AuthorID:  10,
			EditorID:  12,
			CreatedAt: time.Now(),
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Book{
			Id:        102,
			Title:     "book 3",
			AuthorID:  11,
			EditorID:  11,
			CreatedAt: time.Now(),
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&BookGenre{
			BookId:       100,
			GenreId:      1,
			Genre_Rating: 999,
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&BookGenre{
			BookId:       100,
			GenreId:      2,
			Genre_Rating: 9999,
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&BookGenre{
			BookId:       101,
			GenreId:      1,
			Genre_Rating: 99999,
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Translation{
			Id:     1000,
			BookId: 100,
			Lang:   "ru",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Translation{
			Id:     1001,
			BookId: 100,
			Lang:   "md",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Translation{
			Id:     1002,
			BookId: 101,
			Lang:   "ua",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Comment{
			TrackableId:   100,
			TrackableType: "book",
			Text:          "comment1",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Comment{
			TrackableId:   100,
			TrackableType: "book",
			Text:          "comment2",
		})
		Expect(err).NotTo(HaveOccurred())

		err = db.Create(&Comment{
			TrackableId:   1000,
			TrackableType: "translation",
			Text:          "comment3",
		})
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("struct model", func() {
		It("supports HasOne, HasMany, HasMany2Many, Polymorphic, HasMany -> Polymorphic", func() {
			var book Book
			err := db.Model(&book).
				Column("book.id", "Author.id", "Editor.id", "Genres.id", "Comments", "Translations", "Translations.Comments").
				First()
			Expect(err).NotTo(HaveOccurred())

			Expect(book.Id).To(Equal(100))
			Expect(book.Author.ID).To(Equal(10))

			Expect(book.Genres).To(HaveLen(2))
			genre := book.Genres[0]
			Expect(genre.Id).To(Equal(1))
			genre = book.Genres[1]
			Expect(genre.Id).To(Equal(2))

			Expect(book.Translations).To(HaveLen(2))
			translation := book.Translations[0]
			Expect(translation.Id).To(Equal(1000))
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("ru"))

			Expect(translation.Comments).To(HaveLen(1))
			comment := translation.Comments[0]
			Expect(comment.Text).To(Equal("comment3"))

			translation = book.Translations[1]
			Expect(translation.Id).To(Equal(1001))
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("md"))
			Expect(translation.Comments).To(HaveLen(0))

			Expect(book.Comments).To(HaveLen(2))
			comment = book.Comments[0]
			Expect(comment.Text).To(Equal("comment1"))
			comment = book.Comments[1]
			Expect(comment.Text).To(Equal("comment2"))
		})

		It("supports HasMany -> HasOne, HasMany -> HasMany", func() {
			var author Author
			err := db.Model(&author).
				Column("author.*", "Books.Author", "Books.Editor", "Books.Translations").
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(author.ID).To(Equal(10))

			Expect(author.Books).To(HaveLen(2))

			book := &author.Books[0]
			Expect(book.Id).To(Equal(100))
			Expect(book.Author.ID).To(Equal(10))
			Expect(book.Editor.ID).To(Equal(11))

			Expect(book.Translations).To(HaveLen(2))
			translation := book.Translations[0]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("ru"))
			translation = book.Translations[1]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("md"))

			book = &author.Books[1]
			Expect(book.Id).To(Equal(101))
			Expect(book.Author.ID).To(Equal(10))
			Expect(book.Editor.ID).To(Equal(12))

			Expect(book.Translations).To(HaveLen(1))
			translation = book.Translations[0]
			Expect(translation.BookId).To(Equal(101))
			Expect(translation.Lang).To(Equal("ua"))
		})

		It("supports HasMany -> HasMany -> HasMany", func() {
			var genre Genre
			err := db.Model(&genre).
				Column("genre.id", "Books.id", "Books.Translations").
				First()
			Expect(err).NotTo(HaveOccurred())
			Expect(genre.Id).To(Equal(1))
			Expect(genre.Rating).To(Equal(0))

			Expect(genre.Books).To(HaveLen(2))
			book := &genre.Books[0]
			Expect(book.Id).To(Equal(100))

			Expect(book.Translations).To(HaveLen(2))
			translation := book.Translations[0]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("ru"))
			translation = book.Translations[1]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("md"))

			Expect(genre.Books).To(HaveLen(2))
			book = &genre.Books[1]
			Expect(book.Id).To(Equal(101))

			Expect(book.Translations).To(HaveLen(1))
			translation = book.Translations[0]
			Expect(translation.BookId).To(Equal(101))
			Expect(translation.Lang).To(Equal("ua"))
		})
	})

	Describe("slice model", func() {
		It("supports HasOne, HasMany, HasMany2Many", func() {
			var books []Book
			err := db.Model(&books).
				Column("book.id", "Author", "Editor", "Translations", "Genres").
				Order("book.id ASC").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(books).To(HaveLen(3))

			book := &books[0]
			Expect(book.Id).To(Equal(100))
			Expect(book.Author).NotTo(BeNil())
			Expect(book.Author.ID).To(Equal(10))

			Expect(book.Translations).To(HaveLen(2))
			translation := book.Translations[0]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("ru"))
			translation = book.Translations[1]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("md"))

			Expect(book.Genres).To(HaveLen(2))
			genre := book.Genres[0]
			Expect(genre.Id).To(Equal(1))
			Expect(genre.Rating).To(Equal(999))
			genre = book.Genres[1]
			Expect(genre.Id).To(Equal(2))
			Expect(genre.Rating).To(Equal(9999))

			book = &books[1]
			Expect(book.Id).To(Equal(101))
			Expect(book.Author.ID).To(Equal(10))

			Expect(book.Translations).To(HaveLen(1))
			translation = book.Translations[0]
			Expect(translation.BookId).To(Equal(101))
			Expect(translation.Lang).To(Equal("ua"))

			Expect(book.Genres).To(HaveLen(1))
			genre = book.Genres[0]
			Expect(genre.Id).To(Equal(1))
			Expect(genre.Rating).To(Equal(99999))

			book = &books[2]
			Expect(book.Id).To(Equal(102))
			Expect(book.Author.ID).To(Equal(11))

			Expect(book.Translations).To(HaveLen(0))

			Expect(book.Genres).To(HaveLen(0))
		})

		It("supports HasMany2Many, HasMany2Many -> HasMany", func() {
			var genres []Genre
			err := db.Model(&genres).
				Column("genre.*", "Books", "Books.Translations").
				Order("genre.id").
				Select()
			Expect(err).NotTo(HaveOccurred())
			Expect(genres).To(HaveLen(2))

			genre := &genres[0]
			Expect(genre.Id).To(Equal(1))
			Expect(genre.Books).To(HaveLen(2))

			book := genre.Books[0]
			Expect(book.Id).To(Equal(100))

			Expect(book.Translations).To(HaveLen(2))
			translation := book.Translations[0]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("ru"))
			translation = book.Translations[1]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("md"))

			book = genre.Books[1]
			Expect(book.Id).To(Equal(101))

			Expect(book.Translations).To(HaveLen(1))
			translation = book.Translations[0]
			Expect(translation.BookId).To(Equal(101))
			Expect(translation.Lang).To(Equal("ua"))

			genre = &genres[1]
			Expect(genre.Id).To(Equal(2))

			Expect(genre.Books).To(HaveLen(1))
			book = genre.Books[0]
			Expect(book.Id).To(Equal(100))

			Expect(book.Translations).To(HaveLen(2))
			translation = book.Translations[0]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("ru"))
			translation = book.Translations[1]
			Expect(translation.BookId).To(Equal(100))
			Expect(translation.Lang).To(Equal("md"))
		})
	})

	It("filters by HasOne", func() {
		var books []Book
		err := db.Model(&books).
			Column("book.id", "Author._").
			Where("author.id = 10").
			Order("book.id ASC").
			Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(books).To(HaveLen(2))
		Expect(books[0].Id).To(Equal(100))
		Expect(books[0].Author).To(BeNil())
		Expect(books[1].Id).To(Equal(101))
		Expect(books[1].Author).To(BeNil())
	})
})
