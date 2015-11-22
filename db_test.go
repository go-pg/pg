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

type ModRole struct {
	Id   int64 `pg:",nullempty"`
	Name string
}

type ModUser struct {
	Id     int64 `pg:",nullempty"`
	Name   string
	RoleId int64
	Role   *ModRole `pg:"-"`
}

type ModArticle struct {
	Id     int64 `pg:",nullempty"`
	Title  string
	UserId int64    `pg:",nullempty"`
	User   *ModUser `pg:"-"`
}

var _ = Describe("Model", func() {
	It("panics when model with empty name is registered", func() {
		fn := func() {
			pg.NewModel(&ModArticle{}, "").HasOne(&ModArticle{}, "")
		}
		Expect(fn).To(Panic())
	})

	It("panics when model with same name is registered", func() {
		fn := func() {
			pg.NewModel(&ModArticle{}, "a").HasOne(&ModArticle{}, "a")
		}
		Expect(fn).To(Panic())
	})

	It("returns fields and values", func() {
		article := &ModArticle{
			Title: "article title",
		}
		model := pg.NewModel(article, "a")

		q, err := pg.FormatQ(`?Fields`, model)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(q)).To(Equal(`title`))

		q, err = pg.FormatQ(`?Values`, model)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(q)).To(Equal(`'article title'`))
	})

	It("returns columns", func() {
		article := &ModArticle{}
		model := pg.NewModel(article, "a").HasOne("User", "u").HasOne("User.Role", "r")

		q, err := pg.FormatQ(`?Columns`, model)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(q)).To(ContainSubstring(`a.id AS a__id, a.title AS a__title, a.user_id AS a__user_id`))
		Expect(string(q)).To(ContainSubstring(`u.id AS u__id, u.name AS u__name, u.role_id AS u__role_id`))
		Expect(string(q)).To(ContainSubstring(`r.id AS r__id, r.name AS r__name`))
	})

	It("can be used for formatting", func() {
		article := &ModArticle{
			Title: "article title",
		}
		user := &ModUser{
			Name: "user name",
		}
		model := pg.NewModel(article, "a").HasOne(user, "u")

		q, err := pg.FormatQ(`?id ?title ?u__id ?u__name`, model)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(q)).To(Equal(`NULL 'article title' NULL 'user name'`))
	})
})

var _ = Describe("Model on struct", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	It("scans values", func() {
		article := &ModArticle{}
		model := pg.NewModel(article, "a").HasOne("User", "u").HasOne("User.Role", "r")

		_, err := db.QueryOne(model, `
			SELECT
				1 AS a__id, 'article title' AS a__title, 2 AS a__user_id,
				101 AS u__id, 'user name' AS u__name, 102 AS u__role_id,
				201 AS r__id, 'role name' AS r__name
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(*article).To(Equal(ModArticle{
			Id:     1,
			Title:  "article title",
			UserId: 2,
			User: &ModUser{
				Id:     101,
				Name:   "user name",
				RoleId: 102,
				Role: &ModRole{
					Id:   201,
					Name: "role name",
				},
			},
		}))
	})
})

var _ = Describe("Model on slice", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	It("scans values", func() {
		var articles []ModArticle
		model := pg.NewModel(&articles, "a").HasOne("User", "u").HasOne("User.Role", "r")

		_, err := db.Query(model, `
			SELECT
				1 AS a__id, 'article title' AS a__title, 2 AS a__user_id,
				101 AS u__id, 'user name' AS u__name, 102 AS u__role_id,
				201 AS r__id, 'role name' AS r__name
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(articles).To(HaveLen(1))
		Expect(articles[0]).To(Equal(ModArticle{
			Id:     1,
			Title:  "article title",
			UserId: 2,
			User: &ModUser{
				Id:     101,
				Name:   "user name",
				RoleId: 102,
				Role: &ModRole{
					Id:   201,
					Name: "role name",
				},
			},
		}))
	})
})
