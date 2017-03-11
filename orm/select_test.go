package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type SelectModel struct {
	Id       int
	Name     string
	HasOne   *HasOneModel
	HasOneId int
	HasMany  []HasManyModel
}

type HasOneModel struct {
	Id int
}

type HasManyModel struct {
	Id            int
	SelectModelId int
}

var _ = Describe("Select", func() {
	It("works without db", func() {
		q := NewQuery(nil).Where("hello = ?", "world")

		b, err := selectQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT * WHERE (hello = 'world')"))
	})

	It("works with Copy", func() {
		q1 := NewQuery(nil).Where("1 = 1").Where("2 = 2").Where("3 = 3")
		q2 := q1.Copy().Where("q2 = ?", "v2")
		q1 = q1.Where("q1 = ?", "v1")

		b, err := selectQuery{Query: q2}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT * WHERE (1 = 1) AND (2 = 2) AND (3 = 3) AND (q2 = 'v2')"))
	})

	It("specifies all columns", func() {
		q := NewQuery(nil, &SelectModel{})

		b, err := selectQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model"`))
	})

	It("omits columns in main query", func() {
		q := NewQuery(nil, &SelectModel{}).Column("_", "HasOne")

		b, err := selectQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("omits columns in join query", func() {
		q := NewQuery(nil, &SelectModel{}).Column("HasOne._")

		b, err := selectQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("specifies all columns for has one", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).Column("HasOne")

		b, err := selectQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id", "has_one"."id" AS "has_one__id" FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})

	It("specifies all columns for has many", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).Column("HasMany")

		q, err := q.model.GetJoin("HasMany").manyQuery(nil)
		Expect(err).NotTo(HaveOccurred())

		b, err := selectQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "has_many_model"."id", "has_many_model"."select_model_id" FROM "has_many_models" AS "has_many_model" WHERE (("has_many_model"."select_model_id") IN ((1)))`))
	})

	It("supports multiple groups", func() {
		q := NewQuery(nil).Group("one").Group("two")
		b, err := selectQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * GROUP BY "one", "two"`))
	})

	It("WhereOr", func() {
		q := NewQuery(nil).Where("1 = 1").WhereOr("1 = 2")
		b, err := selectQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * WHERE (1 = 1) OR (1 = 2)`))
	})
})

var _ = Describe("Count", func() {
	It("removes LIMIT, OFFSET, and ORDER", func() {
		q := NewQuery(nil).Order("order").Limit(1).Offset(2)

		b, err := q.countSelectQuery("count(*)").AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT count(*)`))
	})

	It("removes LIMIT, OFFSET, and ORDER from CTE", func() {
		q := NewQuery(nil).
			Column("col1", "col2").
			Order("order").
			Limit(1).
			Offset(2).
			WrapWith("wrapper").
			Table("wrapper")

		b, err := q.countSelectQuery("count(*)").AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT "col1", "col2") SELECT count(*) FROM "wrapper"`))
	})

	It("uses CTE when query contains GROUP BY", func() {
		q := NewQuery(nil).Group("one")

		b, err := q.countQuery().countSelectQuery("count(*)").AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT * GROUP BY "one") SELECT count(*) FROM "wrapper"`))
	})

	It("includes has one joins", func() {
		q := NewQuery(nil, &SelectModel{Id: 1}).Column("HasOne")

		b, err := q.countSelectQuery("count(*)").AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT count(*) FROM "select_models" AS "select_model" LEFT JOIN "has_one_models" AS "has_one" ON "has_one"."id" = "select_model"."has_one_id"`))
	})
})

var _ = Describe("With", func() {
	It("WrapWith wraps query in CTE", func() {
		q := NewQuery(nil, &SelectModel{}).
			Where("cond1").
			WrapWith("wrapper").
			Table("wrapper").
			Where("cond2")

		b, err := selectQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT "select_model"."id", "select_model"."name", "select_model"."has_one_id" FROM "select_models" AS "select_model" WHERE (cond1)) SELECT * FROM "wrapper" WHERE (cond2)`))
	})

	It("generates nested CTE", func() {
		q1 := NewQuery(nil).Table("q1")
		q2 := NewQuery(nil).With("q1", q1).Table("q2", "q1")
		q3 := NewQuery(nil).With("q2", q2).Table("q3", "q2")

		b, err := selectQuery{Query: q3}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "q2" AS (WITH "q1" AS (SELECT * FROM "q1") SELECT * FROM "q2", "q1") SELECT * FROM "q3", "q2"`))
	})
})

type orderTest struct {
	order string
	query string
}

var _ = Describe("Select Order", func() {
	orderTests := []orderTest{
		{"id", `"id"`},
		{"id asc", `"id" asc`},
		{"id desc", `"id" desc`},
		{"id ASC", `"id" ASC`},
		{"id DESC", `"id" DESC`},
		{"id ASC NULLS FIRST", `"id ASC NULLS FIRST"`},
	}

	It("sets order", func() {
		for _, test := range orderTests {
			q := NewQuery(nil).Order(test.order)

			b, err := selectQuery{Query: q}.AppendQuery(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(`SELECT * ORDER BY ` + test.query))
		}
	})
})
