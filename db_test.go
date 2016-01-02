package pg_test

import (
	"net"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/pg.v3"
)

func TestPG(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pg")
}

func pgOptions() *pg.Options {
	return &pg.Options{
		User:     "postgres",
		Database: "postgres",
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

	It("supports slice of values", func() {
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

	It("supports Collection", func() {
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

	Describe("with WithTimeout", func() {
		It("slow query passes", func() {
			_, err := db.WithTimeout(time.Minute).Exec(`SELECT pg_sleep(1)`)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("Listener.ReceiveTimeout", func() {
	var db *pg.DB
	var ln *pg.Listener

	BeforeEach(func() {
		opt := pgOptions()
		opt.PoolSize = 1
		db = pg.Connect(opt)

		var err error
		ln, err = db.Listen("test_channel")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("reuses connection", func() {
		for i := 0; i < 100; i++ {
			_, _, err := ln.ReceiveTimeout(time.Millisecond)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(MatchRegexp(".+ i/o timeout"))
		}
	})
})

type Role struct {
	Id   int64
	Name string

	Authors []Author
}

type Author struct {
	Id     int64
	Name   string
	RoleId int64
	Role   *Role

	Entries []Entry
}

type Entry struct {
	Id       int64
	Title    string
	AuthorId int64
	Author   *Author
	EditorId int64
	Editor   *Author
}

var _ = Describe("Select", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())

		sql := []string{
			`DROP TABLE IF EXISTS role`,
			`DROP TABLE IF EXISTS author`,
			`DROP TABLE IF EXISTS entry`,
			`CREATE TABLE role(id int, name text)`,
			`CREATE TABLE author(id int, name text, role_id int)`,
			`CREATE TABLE entry(id int, title text, author_id int, editor_id int)`,
			`INSERT INTO role VALUES (1, 'role 1')`,
			`INSERT INTO role VALUES (2, 'role 2')`,
			`INSERT INTO author VALUES (10, 'user 1', 1)`,
			`INSERT INTO author VALUES (11, 'user 2', 2)`,
			`INSERT INTO entry VALUES (100, 'entry 1', 10, 11)`,
			`INSERT INTO entry VALUES (101, 'entry 2', 10, 11)`,
			`INSERT INTO entry VALUES (102, 'entry 3', 11, 11)`,
		}
		for _, q := range sql {
			_, err := db.Exec(q)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	Describe("struct", func() {
		It("joins HasOne", func() {
			var entry Entry
			err := db.Select("entry.id", "author.id", "editor.id", "author.role.id").
				First(&entry).
				Err()
			Expect(err).NotTo(HaveOccurred())
			Expect(entry.Id).To(Equal(int64(100)))
			Expect(entry.Author.Id).To(Equal(int64(10)))
			Expect(entry.Author.Role.Id).To(Equal(int64(1)))
		})

		It("joins HasMany", func() {
			var role Role
			err := db.Select("role.id", "authors.id", "authors.entries.id").
				First(&role).
				Err()
			Expect(err).NotTo(HaveOccurred())
			Expect(role.Id).To(Equal(int64(1)))

			Expect(role.Authors).To(HaveLen(1))
			author := &role.Authors[0]
			Expect(author.Id).To(Equal(int64(10)))
			Expect(author.Entries).To(HaveLen(2))

			Expect(author.Entries).To(HaveLen(2))
			entry := &author.Entries[0]
			Expect(entry.Id).To(Equal(int64(100)))
			entry = &author.Entries[1]
			Expect(entry.Id).To(Equal(int64(101)))
		})
	})

	Describe("slice", func() {
		It("joins HasOne", func() {
			var entries []Entry
			err := db.Select("entry.id", "author", "editor", "author.role").
				Order("role.id").
				Find(&entries).
				Err()
			Expect(err).NotTo(HaveOccurred())
			Expect(entries).To(HaveLen(3))

			entry := &entries[0]
			Expect(entry.Id).To(Equal(int64(100)))
			Expect(entry.Author.Id).To(Equal(int64(10)))
			Expect(entry.Author.Role.Id).To(Equal(int64(1)))

			entry = &entries[1]
			Expect(entry.Id).To(Equal(int64(101)))
			Expect(entry.Author.Id).To(Equal(int64(10)))
			Expect(entry.Author.Role.Id).To(Equal(int64(1)))

			entry = &entries[2]
			Expect(entry.Id).To(Equal(int64(102)))
			Expect(entry.Author.Id).To(Equal(int64(11)))
			Expect(entry.Author.Role.Id).To(Equal(int64(2)))
		})

		It("joins HasMany", func() {
			var roles []Role
			err := db.Select("role.id", "authors", "authors.entries").
				Order("role.id").
				Find(&roles).
				Err()
			Expect(err).NotTo(HaveOccurred())
			Expect(roles).To(HaveLen(2))

			author := &roles[0].Authors[0]
			Expect(author.Id).To(Equal(int64(10)))
			Expect(author.Entries).To(HaveLen(2))

			author = &roles[1].Authors[0]
			Expect(author.Id).To(Equal(int64(11)))
			Expect(author.Entries).To(HaveLen(1))
		})
	})

	It("Last returns last row", func() {
		var entry Entry
		err := db.Select("entry.id").
			Last(&entry).
			Err()
		Expect(err).NotTo(HaveOccurred())
		Expect(entry.Id).To(Equal(int64(102)))
	})

	PIt("Count returns number of rows", func() {
		var count int
		err := db.Select().Model(&Entry{}).Count(&count).Err()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(3))
	})

	It("joins specified columns", func() {
		var entry Entry
		err := db.Select("entry.id", "author.id", "author.name").
			First(&entry).
			Err()
		Expect(err).NotTo(HaveOccurred())
		Expect(entry.Id).To(Equal(int64(100)))
		Expect(entry.Author.Id).To(Equal(int64(10)))
		Expect(entry.Author.Name).To(Equal("user 1"))
		Expect(entry.Author.RoleId).To(BeZero())
	})

	It("filters by HasOne", func() {
		var entries []Entry
		err := db.Select("entry.id").Where("? = 10", pg.F("author.id")).
			Order("entry.id").
			Find(&entries).
			Err()
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(2))
		Expect(entries[0].Id).To(Equal(int64(100)))
		Expect(entries[0].Author).To(BeNil())
		Expect(entries[1].Id).To(Equal(int64(101)))
		Expect(entries[1].Author).To(BeNil())
	})
})
